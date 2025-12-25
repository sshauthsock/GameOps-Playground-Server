#include <iostream> 
#include <cstdlib>      
#include <unistd.h>     
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <sys/epoll.h>  
#include <vector>       
#include <map>          
#include <cstring>      // memcpy 함수용
#include <algorithm>
#include <memory>
#include "websocket_server.h"

#define MAX_EVENTS 10
#define BUFFER_SIZE 1024 
#define HEADER_SIZE 4

enum class RoomState
{
    LOBBY,  // 0
    INGAME  // 1
};

struct Player
{
    int fd;         // 이 플레이어의 '손님 번호표'
    std::string name; // 이 플레이어의 이름
    
    // 인게임 상태
    int hp;
    bool is_dead;
    // (M5에서는 여기에 float x, float y; 같은 위치 변수도 들어갑니다)
};

struct Room
{
    int room_id;            // 방 고유 ID
    std::string room_name;  // 방 이름
    int host_player_fd;     // 현재 방장의 FD
    //std::vector<int> players_fds; // 방에 속한 모든 플레이어의 FD 목록
    std::vector<Player> players;   // <-- 이 줄로 '대체'합니다.
    std::map<int, bool> player_ready_states;
    RoomState state;
    int current_turn_index;

};



// ===============================================
// [M2 핵심] 서버 전역 데이터 (모든 함수가 공유)
// ===============================================
// '손님 FD'와 '개인 수신 가방'을 짝지어 관리하는 맵
std::map<int, std::vector<char>> client_buffers;
// '방 ID'를 'Room 객체'에 연결(매핑)하는 방 목록
std::map<int, Room> global_rooms;
// '플레이어 FD'가 '어느 방 ID'에 있는지
std::map<int, int> player_room_map;
// '손님 FD'를 '플레이어 이름'에 연결하는 맵
std::map<int, std::string> global_player_names;
// 다음 방 ID 발급을 위한 카운터
int next_room_id = 1; 
// WebSocket 서버 전역 변수
std::unique_ptr<WebSocketServer> ws_server;
std::map<int, bool> is_websocket_client;  // 클라이언트가 WebSocket인지 구분
// ===============================================


// ===============================================
// [!!] 송신용 메시지 조립 함수
// (리틀 엔디언 변환 로직 포함)
// ===============================================
void SendMessage(int client_fd, unsigned short message_id, const std::vector<char>& payload)
{
    // 1. 총 길이 계산: 헤더(4) + 페이로드 길이
    unsigned short total_length = HEADER_SIZE + (unsigned short)payload.size();
    
    // 2. 메시지 조립을 위한 최종 벡터 생성 (8바이트)
    std::vector<char> message(total_length);

    // 3. 헤더 조립
    // 3-1. 총 길이 (2바이트) 넣기 - 리틀 엔디언
    message[0] = (char)(total_length & 0xFF);
    message[1] = (char)((total_length >> 8) & 0xFF);

    // 3-2. 메시지 ID (2바이트) 넣기 - 리틀 엔디언
    message[2] = (char)(message_id & 0xFF);
    message[3] = (char)((message_id >> 8) & 0xFF);

    // 4. 페이로드 복사
    if (!payload.empty()) {
        std::memcpy(&message[HEADER_SIZE], payload.data(), payload.size());
    }

    // WebSocket 클라이언트인지 확인
    if (is_websocket_client[client_fd] && ws_server)
    {
        // WebSocket으로 전송
        ws_server->send_to_client(client_fd, message);
        std::cout << "  -> [WebSocket] ID " << message_id << " 전송 완료" << std::endl;
        return;
    }

    // 기존 TCP 전송
    if (write(client_fd, message.data(), message.size()) < 0)
    {
        std::cerr << "클라이언트(FD: " << client_fd << ")에게 ID " << message_id << " 응답 송신 실패!" << std::endl;
    }
    else
    {
        std::cout << "  -> ID " << message_id << " 응답 성공적으로 송신함 (" << message.size() << " 바이트)." << std::endl;
    }
}


// 함수 선언 (전방 선언)
void HandleBuffer(int client_fd, std::vector<char>& buffer);

// WebSocket 메시지 핸들러
void HandleWebSocketMessage(int client_fd, const std::vector<char>& data)
{
    // WebSocket 클라이언트로 표시 (FD가 10000 이상이면 WebSocket 클라이언트)
    if (!is_websocket_client[client_fd])
    {
        is_websocket_client[client_fd] = true;
        client_buffers[client_fd] = std::vector<char>();
        std::cout << "[WebSocket] 클라이언트 등록 완료 (FD: " << client_fd << ")" << std::endl;
    }
    
    // WebSocket 메시지를 기존 TCP 서버의 HandleBuffer 함수로 전달
    // client_fd를 WebSocket 전용 FD로 매핑
    std::vector<char>& buffer = client_buffers[client_fd];
    buffer.insert(buffer.end(), data.begin(), data.end());
    HandleBuffer(client_fd, buffer);
}

void CleanupPlayer(int client_fd, bool close_websocket_session = true)
{
    std::cout << "[플레이어 정리 시작] (FD: " << client_fd << ")" << std::endl;
    
    // WebSocket 클라이언트인지 확인
    // cleanup 콜백에서 호출된 경우 이미 세션이 제거되는 중이므로 중복 제거 방지
    // cleanup 콜백은 close_client 호출 전에 호출되므로, 세션은 아직 존재함
    // 하지만 cleanup 콜백 호출 후 close_client가 호출되므로, 여기서는 세션 제거를 하지 않음
    if (close_websocket_session && is_websocket_client[client_fd] && ws_server)
    {
        ws_server->close_client(client_fd);
        is_websocket_client.erase(client_fd);
    }
    else if (!close_websocket_session && is_websocket_client[client_fd])
    {
        // cleanup 콜백에서 호출된 경우: 세션은 close_client에서 제거되므로 여기서는 is_websocket_client만 제거
        is_websocket_client.erase(client_fd);
    }
    
    // 1. [정리 1] '이름 맵'에서 제거
    global_player_names.erase(client_fd);

    // 2. '조견표'에서 이 플레이어가 속한 방 ID 찾기
    auto map_it = player_room_map.find(client_fd);
    
    if (map_it == player_room_map.end())
    {
        std::cout << "    -> 로비에서 접속 끊음. 추가 정리 없음." << std::endl;
        return; 
    }

    // --- (여기부터는 '방'에 속한 유저) ---
    int room_id = map_it->second;
    Room& target_room = global_rooms[room_id]; 
    
    bool was_host = (target_room.host_player_fd == client_fd);
    int new_host_fd = 0; 

    // 4. [정리 2] Room의 '플레이어 목록'에서 '나'를 제거 (M3 수정)
    // [핵심 수정] 게임 중 연결 종료 시에도 완전히 제거되도록 보장
    std::vector<Player>& players = target_room.players; // [M3 수정]
    
    // 제거 전 플레이어 수 기록
    size_t players_before = players.size();
    
    auto it = std::remove_if(players.begin(), players.end(), [client_fd](const Player& p) {
        return p.fd == client_fd;
    });
    int erase_count = std::distance(it, players.end());
    players.erase(it, players.end());

    if (erase_count > 0)
    {
         std::cout << "    -> " << room_id << "번 방에서 (FD: " << client_fd << ") 제거 완료. (제거 전: " << players_before << "명, 제거 후: " << players.size() << "명)" << std::endl;
    }
    else
    {
        // [핵심 수정] 제거되지 않은 경우 경고 (유령 플레이어 가능성)
        std::cerr << "    -> [경고] " << room_id << "번 방에서 (FD: " << client_fd << ")를 찾지 못했습니다. 유령 플레이어일 수 있습니다." << std::endl;
        std::cerr << "    -> 현재 플레이어 목록 (총 " << players.size() << "명):" << std::endl;
        for (const Player& p : players)
        {
            std::cerr << "       - FD: " << p.fd << ", Name: " << p.name << std::endl;
        }
    }

    // 5. [정리 3] '준비 상태' 맵에서 '나'를 제거
    target_room.player_ready_states.erase(client_fd);

    // 6. [정리 4] '조견표'에서도 '나'를 제거
    player_room_map.erase(client_fd);

    // 7. [방장 이전 로직 및 빈 방 삭제]
    if (was_host)
    {
        std::cout << "   -> [방장 이전] (FD: " << client_fd << ")가 방장이었음!" << std::endl;
        if (players.empty()) // 7-A. 방이 텅 비었음
        {
            std::cout << "   -> 방이 비었으므로 " << room_id << "번 방을 삭제." << std::endl;
            global_rooms.erase(room_id);
            new_host_fd = 0; 
        }
        else // 7-B. 방에 남은 사람이 있음
        {
            new_host_fd = players[0].fd; // [M3 수정] .fd
            target_room.host_player_fd = new_host_fd; 
            std::cout << "   -> 새 방장 임명 (FD: " << new_host_fd << ")" << std::endl;
        }
    }
    else 
    {
        // 방장이 아닌 플레이어가 나갔을 때도 빈 방 확인
        if (players.empty())
        {
            std::cout << "   -> [빈 방 삭제] 모든 플레이어가 나갔으므로 " << room_id << "번 방을 삭제." << std::endl;
            global_rooms.erase(room_id);
            new_host_fd = 0;
        }
        else
        {
            new_host_fd = target_room.host_player_fd; // 기존 방장 유지
        }
    }

    // 8. [방송] '방에 남은 사람'들에게 ID 317 '방송'
    // [핵심 수정] 연결이 끊어진 클라이언트(client_fd)는 제외하고 브로드캐스트
    if (new_host_fd != 0) 
    {
        std::vector<char> notify_payload(sizeof(int) * 2);
        std::memcpy(notify_payload.data(), &client_fd, sizeof(int)); 
        std::memcpy(notify_payload.data() + sizeof(int), &new_host_fd, sizeof(int));

        // [핵심 수정] 연결이 끊어진 클라이언트를 제외하고 브로드캐스트
        // players 벡터에서 이미 client_fd를 제거했지만, 안전을 위해 명시적으로 확인
        for (const Player& p : players) 
        {
            // 연결이 끊어진 클라이언트는 제외 (이미 제거되었지만 이중 안전장치)
            if (p.fd != client_fd)
            {
                SendMessage(p.fd, 317, notify_payload);
                std::cout << "    -> [ID 317 전송] FD " << p.fd << "에게 UserLeft Notify 전송" << std::endl;
            }
            else
            {
                std::cout << "    -> [ID 317 건너뜀] 연결이 끊어진 클라이언트(FD: " << client_fd << ")는 제외" << std::endl;
            }
        }
    }
    std::cout << "[플레이어 정리 완료]" << std::endl;
}

// ===============================================
// [!!] 바이트 조각을 float으로 변환하는 헬퍼 함수
// ===============================================
float get_float_le(const char* data) {
    float result;
    // 리틀 엔디언 가정하에, 바이트 배열을 float 메모리 공간에 복사합니다.
    std::memcpy(&result, data, sizeof(float));
    return result;
}

// ===============================================
// [!!] '가방'을 검사하고 '완전한 메시지'를 처리하는 함수
// ===============================================
void HandleBuffer(int client_fd, std::vector<char>& buffer)
{
    while (true)
    {
        // 1. [헤더 검사] (4바이트)
        if (buffer.size() < HEADER_SIZE) 
        {
            return; 
        }

        // 2. [본체 검사] '총 길이' (리틀 엔디언)
        unsigned short packet_length = (unsigned short)(
            (unsigned char)buffer[1] << 8 | (unsigned char)buffer[0]
        );

        // 잘못된 패킷 길이 검증 (최소 헤더 크기, 최대 합리적 크기)
        if (packet_length < HEADER_SIZE || packet_length > 1024 * 1024)  // 1MB 제한
        {
            std::cerr << "[오류] 잘못된 패킷 길이: " << packet_length << " 바이트. 버퍼 정리." << std::endl;
            buffer.clear();
            return;
        }

        if (buffer.size() < packet_length)
        {
            return;
        }

        // --- (여기까지 왔다면 '완전한 메시지' 1개 확보) ---

        // 3. [메시지 ID 읽기]
        unsigned short message_id = (unsigned short)(
            (unsigned char)buffer[3] << 8 | (unsigned char)buffer[2]
        );

        std::cout << "[메시지 처리 시작] ID: " << message_id << " (총 길이: " << packet_length << " 바이트)" << std::endl;
        
        
        // Payload 시작 주소
        const char* payload_data = buffer.data() + HEADER_SIZE;
        std::vector<char> response_payload; // 응답 페이로드 임시 저장소
        
        if (message_id == 100) // C2S_LoginRequest (Payload: char[20])
        {
            // 1. 유저 이름 추출 (인덱스 4부터 20바이트)
            std::string username_str(payload_data, 20);
            std::cout << "  -> ID 100 (로그인 요청) 받음! 유저이름: [" << username_str << "]" << std::endl;
            // ===============================================
            // [M2 추가] 플레이어 이름 저장
            // (main 함수에 std::map<int, std::string> global_player_names; 선언 필요)
            global_player_names[client_fd] = username_str;
            // ===============================================
            // 2. 응답 메시지 ID 101 조립 (Payload: int, 4바이트)
            int login_result_code = 1; // 1: 성공
            // Payload에 int 값(4바이트)을 리틀 엔디언으로 담습니다.
            response_payload.resize(sizeof(int));
            response_payload[0] = (char)(login_result_code & 0xFF);
            response_payload[1] = (char)((login_result_code >> 8) & 0xFF);
            response_payload[2] = (char)((login_result_code >> 16) & 0xFF);
            response_payload[3] = (char)((login_result_code >> 24) & 0xFF);
            
            SendMessage(client_fd, 101, response_payload);
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        else if (message_id == 200) // C2S_FireCannon (Payload: float X, float Y)
        {
            // 1. float X, Y 추출 (총 8바이트)
            float x = get_float_le(payload_data);
            float y = get_float_le(payload_data + sizeof(float)); // 4바이트 뒤부터 Y 시작

            std::cout << "  -> ID 200 (대포 발사) 받음! 좌표: (" << x << ", " << y << ")" << std::endl;
            
            // 2. 응답 메시지 ID 201 조립 (Payload: int 결과, float X, float Y)
            int fire_result = 0; // 0: 발사 성공
            
            // 응답 페이로드 (4 + 4 + 4 = 12 바이트)
            response_payload.resize(12);

            // 2-1. int 결과 (4바이트) 복사
            std::memcpy(response_payload.data(), &fire_result, sizeof(int));

            // 2-2. float X, Y (각 4바이트) 복사 (들어온 값을 그대로 돌려준다고 가정)
            std::memcpy(response_payload.data() + sizeof(int), &x, sizeof(float));
            std::memcpy(response_payload.data() + sizeof(int) + sizeof(float), &y, sizeof(float));

            SendMessage(client_fd, 201, response_payload);
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        else if (message_id == 290) // C2S_GetRoomListRequest (Payload: 없음)
        {
            std::cout << "  -> ID 290 (방 목록 요청) 받음!" << std::endl;

            // 1. 방 갯수 (int, 4바이트)
            int room_count = (int)global_rooms.size();
            
            // 2. '방 갯수'를 페이로드의 맨 앞에 추가
            response_payload.resize(sizeof(int));
            std::memcpy(response_payload.data(), &room_count, sizeof(int));
            
            // 3. '방 목록' 순회 (C++17 스타일)
            for (auto const& [room_id, room] : global_rooms)
            {
                // '방 1개'의 데이터 크기 = 4(ID) + 20(이름) + 4(인원수) = 28 바이트
                std::vector<char> room_info_bytes(28);

                // 3-1. 방 ID (int, 4바이트)
                std::memcpy(room_info_bytes.data(), &room.room_id, sizeof(int));

                // 3-2. 방 이름 (char[20], 20바이트)
                // std::string을 char[20] 버퍼로 복사 (strncpy 사용)
                // (이름이 20바이트보다 짧으면 나머지는 자동으로 0(\0)으로 채워집니다)
                std::strncpy(room_info_bytes.data() + sizeof(int), 
                             room.room_name.c_str(), 
                             20);
                // 만약 이름이 20자를 넘어도 마지막은 \0으로 보장 (안전을 위해)
                room_info_bytes[sizeof(int) + 19] = '\0'; 

                // 3-3. 방 인원수 (int, 4바이트)
                int player_count = (int)room.players.size();
                std::memcpy(room_info_bytes.data() + sizeof(int) + 20, 
                             &player_count, 
                             sizeof(int));

                // 3-4. 조립된 '방 1개' 정보(28바이트)를 최종 페이로드 맨 뒤에 삽입
                response_payload.insert(response_payload.end(), 
                                        room_info_bytes.begin(), 
                                        room_info_bytes.end());
            }

            // 4. 최종 조립된 '가변 길이' 페이로드로 ID 291 응답 송신
            SendMessage(client_fd, 291, response_payload);
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        
        else if (message_id == 300) // C2S_CreateRoomRequest (Payload: char[20] room_name)
        {
            // 1. 페이로드에서 '방 이름' 추출 (20바이트)
            std::string room_name_str(payload_data, 20);
            // (이름 뒤에 붙은 \0 찌꺼기 제거 - C++ 스타일)
            room_name_str.erase(room_name_str.find('\0')); 
            
            std::cout << "  -> ID 300 (방 만들기) 요청 받음! 방 이름: [" << room_name_str << "]" << std::endl;

            // 2. 새 Room 객체 생성 및 초기화
            Room new_room;
            new_room.room_id = next_room_id; // '다음 방 ID' 사용
            new_room.room_name = room_name_str;
            new_room.host_player_fd = client_fd; // 요청한 사람이 방장
            //new_room.players_fds.push_back(client_fd); // 방장도 플레이어 목록에 추가
            new_room.state = RoomState::LOBBY;
            Player host_player; // '방장' 플레이어 객체 생성
            host_player.fd = client_fd;
            host_player.name = global_player_names[client_fd]; // [M2]에서 저장한 이름
            host_player.hp = 100;    // '기본값' HP
            host_player.is_dead = false; // '기본값' 사망 상태
            
            // [핵심 수정] 중복 추가 방지: 이미 같은 fd를 가진 플레이어가 있는지 확인
            bool already_exists = false;
            for (const Player& p : new_room.players)
            {
                if (p.fd == client_fd)
                {
                    already_exists = true;
                    std::cerr << "    -> [경고] FD " << client_fd << "가 이미 방에 존재합니다. 중복 추가를 방지합니다." << std::endl;
                    break;
                }
            }
            
            if (!already_exists)
            {
                new_room.players.push_back(host_player); // 'players' 벡터에 '객체'를 추가
            }
            // ===============================================
            // [M2 추가] '손님 FD'를 '플레이어 이름'에 연결하는 맵
            //std::map<int, std::string> global_player_names;

            // 3. '방 목록' 맵에 새 방 추가
            //    (C++11 emplace를 쓰면 더 효율적이지만, 지금은 이게 직관적입니다)
            global_rooms[new_room.room_id] = new_room;

            // 4. 다음 방 ID 증가
            next_room_id++;
            // ===============================================
            // [M2 추가] '방 만들기' 성공 시, '조견표'에 등록
            player_room_map[client_fd] = new_room.room_id;
            // ===============================================
            
            // 5. 응답 (ID 301) 조립
            response_payload.resize(sizeof(int));
            
            // 5. 응답 (ID 301) 조립: 방금 만든 'room_id' (int)
            response_payload.resize(sizeof(int));
            std::memcpy(response_payload.data(), &new_room.room_id, sizeof(int));

            SendMessage(client_fd, 301, response_payload);
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        else if (message_id == 310) // C2S_JoinRoomRequest (Payload: int room_id)
        {
            // 1. 페이로드에서 '참가할 방 ID' 추출
            int requested_room_id;
            std::memcpy(&requested_room_id, payload_data, sizeof(int));
            
            std::cout << "  -> ID 310 (방 참가) 요청 받음! (Room ID: " << requested_room_id << ")" << std::endl;

            // 2. '방'이 존재하는지 '키'로 조회 (find)
            auto it = global_rooms.find(requested_room_id);
            
            // 3. 응답 (ID 311) 페이로드 준비 (bool success, int room_id) - 총 5바이트
            // (bool은 C++에서 1바이트입니다)
            response_payload.resize(sizeof(bool) + sizeof(int));
            bool success = false;
            int response_room_id = 0;

            if (it == global_rooms.end()) // 4-A. 방이 없음 (실패)
            {
                std::cout << "    -> 실패: ID " << requested_room_id << " 방이 존재하지 않음." << std::endl;
                success = false;
                response_room_id = 0;
            }
            else // 4-B. 방이 있음 (성공)
            {
                std::cout << "    -> 성공: ID " << requested_room_id << " 방에 참가." << std::endl;
                success = true;
                response_room_id = requested_room_id;

                // 5. [핵심] Room 객체에 '나'를 추가
                Room& target_room = it->second; 
                //target_room.players_fds.push_back(client_fd);
                Player host_player; // '방장' 플레이어 객체 생성
                host_player.fd = client_fd;
                host_player.name = global_player_names[client_fd]; // [M2]에서 저장한 이름
                host_player.hp = 100;    // '기본값' HP
                host_player.is_dead = false; // '기본값' 사망 상태
            
                // [핵심 수정] 중복 추가 방지: 이미 같은 fd를 가진 플레이어가 있는지 확인
                bool already_exists = false;
                for (const Player& p : target_room.players)
                {
                    if (p.fd == client_fd)
                    {
                        already_exists = true;
                        std::cerr << "    -> [경고] FD " << client_fd << "가 이미 방에 존재합니다. 중복 추가를 방지합니다." << std::endl;
                        break;
                    }
                }
                
                if (!already_exists)
                {
                    target_room.players.push_back(host_player); // 'players' 벡터에 '객체'를 추가
                }
            // ===============================================

                // ===============================================
                // [M2 추가] '방 참가' 성공 시, '조견표'에 등록
                player_room_map[client_fd] = requested_room_id;
                // ===============================================

                // 6. [핵심] '방 안의 다른 사람'들에게 ID 312 '방송'
                // 6-1. ID 312 페이로드 조립 (int player_id, char[20] player_name)
                std::vector<char> notify_payload(sizeof(int) + 20);
                
                // 6-2. player_id (방금 들어온 '나'의 FD)
                std::memcpy(notify_payload.data(), &client_fd, sizeof(int));
                
                // 6-3. player_name (방금 들어온 '나'의 이름)
                std::string my_name = global_player_names[client_fd];
                std::strncpy(notify_payload.data() + sizeof(int), my_name.c_str(), 20);
                notify_payload[sizeof(int) + 19] = '\0'; // 안전장치
                
                // 6-4. '나'를 뺀 '기존 방 멤버'에게 '순회'하며 '방송'
                for (const Player& p : target_room.players)
                {
                    if (p.fd != client_fd) // '나'는 빼고 (Player 객체의 fd와 비교)
                    {
                        SendMessage(p.fd, 312, notify_payload);
                    }
                }
            }

            // 7. '나'에게 최종 응답 (ID 311) 송신
            std::memcpy(response_payload.data(), &success, sizeof(bool));
            std::memcpy(response_payload.data() + sizeof(bool), &response_room_id, sizeof(int));
            SendMessage(client_fd, 311, response_payload);
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        else if (message_id == 315) // C2S_LeaveRoomRequest (Payload: 없음)
        {
            std::cout << "  -> ID 315 (방 나가기) 요청 받음! (FD: " << client_fd << ")" << std::endl;

            // 1. '조견표'에서 이 플레이어가 속한 방 ID 찾기
            auto map_it = player_room_map.find(client_fd);
            if (map_it == player_room_map.end())
            {
                // (방에 있지도 않은 유저가 '나가기'를 보낸 비정상 상황)
                std::cerr << "    -> 오류: 방에 속하지 않은 유저(FD: " << client_fd << ")가 나가기 요청." << std::endl;
                // (이럴땐 ID 316 실패 응답을 보내는 것이 좋지만, 지금은 무시)
                buffer.erase(buffer.begin(), buffer.begin() + packet_length);
                continue; // (HandleBuffer의 다음 루프로)
            }
            
            int room_id = map_it->second;
            Room& target_room = global_rooms[room_id]; // (방은 반드시 존재함)
            
            bool was_host = (target_room.host_player_fd == client_fd);
            int new_host_fd = 0; // 0은 '방장 없음' 또는 '방 폭파'를 의미

            // 2. [핵심] Room의 '플레이어 목록'에서 '나'를 제거
            std::vector<Player>& players = target_room.players; // [수정 1]
            auto it = std::remove_if(players.begin(), players.end(), [client_fd](const Player& p) {
                return p.fd == client_fd;
            });
            int erase_count = std::distance(it, players.end());
            players.erase(it, players.end());
            if (erase_count > 0)
            {
                std::cout << "    -> " << room_id << "번 방에서 (FD: " << client_fd << ") 제거 완료." << std::endl;
            }

            // 3. '조견표'에서도 '나'를 제거
            player_room_map.erase(client_fd);

            // 4. [방장 이전 로직]
            if (was_host)
            {
                std::cout << "    -> [방장 이전] 나간 사람(FD: " << client_fd << ")이 방장이었음!" << std::endl;
                
                if (players.empty()) // 4-A. 방이 텅 비었음
                {
                    std::cout << "    -> 방이 비었으므로 " << room_id << "번 방을 삭제합니다." << std::endl;
                    global_rooms.erase(room_id);
                    new_host_fd = 0; // (방송할 사람도 없지만, 명시적으로 0)
                }
                else // 4-B. 방에 남은 사람이 있음
                {
                    // 새 방장 = 목록의 맨 앞 사람
                    new_host_fd = players[0].fd; // [수정] .fd를 꺼냅니다.
                    target_room.host_player_fd = new_host_fd;
                    std::cout << "    -> 새 방장 임명 (FD: " << new_host_fd << ")" << std::endl;
                }
            }
            else
            {
                // 방장이 나간게 아니라면, 기존 방장 ID 유지
                if (!players.empty()) // (방이 폭파된게 아니라면)
                {
                    new_host_fd = target_room.host_player_fd;
                }
            }
            
            // 5. '나'에게 ID 316 (나가기 성공) 응답
            response_payload.resize(sizeof(bool));
            bool success = true;
            std::memcpy(response_payload.data(), &success, sizeof(bool));
            SendMessage(client_fd, 316, response_payload);
            
            // [핵심 수정] 방 나가기는 연결을 끊는 것이 아니므로 CleanupPlayer를 호출하지 않음
            // CleanupPlayer는 WebSocket 세션을 제거하므로, 방 나가기 후에도 연결을 유지해야 함
            // 대신 필요한 정리만 수행 (이미 위에서 player_room_map에서 제거함)
            std::cout << "    -> [방 나가기 완료] (FD: " << client_fd << ")는 로비로 돌아갔습니다. 세션은 유지됩니다." << std::endl;

            // 6. [방송] '방에 남은 사람'들에게 ID 317 '방송'
            // [핵심 수정] 나간 클라이언트(client_fd)는 제외하고 브로드캐스트
            if (new_host_fd != 0) // (방이 폭파되지 않았다면)
            {
                // ID 317 페이로드 (int player_id, int new_host_player_id)
                std::vector<char> notify_payload(sizeof(int) * 2);
                std::memcpy(notify_payload.data(), &client_fd, sizeof(int)); // 나간 사람
                std::memcpy(notify_payload.data() + sizeof(int), &new_host_fd, sizeof(int)); // 새 방장

                // [핵심 수정] 나간 클라이언트를 제외하고 브로드캐스트
                // players 벡터에서 이미 client_fd를 제거했지만, 안전을 위해 명시적으로 확인
                for (const Player& p : players) // (이미 '나'는 빠진 목록)
                {
                    // 나간 클라이언트는 제외 (이미 제거되었지만 이중 안전장치)
                    if (p.fd != client_fd)
                    {
                        SendMessage(p.fd, 317, notify_payload);
                        std::cout << "    -> [ID 317 전송] FD " << p.fd << "에게 UserLeft Notify 전송" << std::endl;
                    }
                    else
                    {
                        std::cout << "    -> [ID 317 건너뜀] 나간 클라이언트(FD: " << client_fd << ")는 제외" << std::endl;
                    }
                }
 
            }
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        else if (message_id == 320) // C2S_ReadyRequest (Payload: bool is_ready)
        {

            // 1. 페이로드에서 '준비 상태' 추출 (1바이트)
            bool is_ready;
            std::memcpy(&is_ready, payload_data, sizeof(bool));

            std::cout << "  -> ID 320 (준비) 요청 받음! (FD: " << client_fd << ", Ready: " << is_ready << ")" << std::endl;
            
            // 1. [학생이 말한 1단계] '어느 방'에서 보냈는지 찾기
            auto map_it = player_room_map.find(client_fd);
            if (map_it == player_room_map.end())
            {
                std::cerr << "    -> 오류: 방에 속하지 않은 유저(FD: " << client_fd << ")가 준비 요청." << std::endl;
                buffer.erase(buffer.begin(), buffer.begin() + packet_length);
                continue; // (다음 루프로)
            }

            int room_id = map_it->second;
            Room& target_room = global_rooms[room_id];
            if (target_room.state == RoomState::LOBBY){
            // 2. [학생이 말한 2단계] 그 방의 '준비 상태 맵' 갱신
            target_room.player_ready_states[client_fd] = is_ready;
            
            // 3. [학생이 말한 3단계] '방 안의 모든 사람'에게 ID 321 '방송'
            // ID 321 페이로드 (int player_id, bool is_ready) - 총 5바이트
            response_payload.resize(sizeof(int) + sizeof(bool));
            std::memcpy(response_payload.data(), &client_fd, sizeof(int)); // '누가'
            std::memcpy(response_payload.data() + sizeof(int), &is_ready, sizeof(bool)); // '어떻게'

            // '모든' 방 멤버에게 순회하며 방송
            for (const Player& p : target_room.players)
            {
                SendMessage(p.fd, 321, response_payload);
            }
            }
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }

        else if (message_id == 330) // C2S_GameStartRequest (Payload: 없음)
        {
            
            std::cout << "  -> ID 330 (게임 시작) 요청 받음! (FD: " << client_fd << ")" << std::endl;

            // 1. '어느 방'에서 보냈는지 찾기
            auto map_it = player_room_map.find(client_fd);
            if (map_it == player_room_map.end())
            {
                std::cerr << "    -> [검증 실패] 방에 속하지 않은 유저가 시작 요청." << std::endl;
                continue; 
            }
            int room_id = map_it->second;
            Room& target_room = global_rooms[room_id];
            if (target_room.state == RoomState::LOBBY){
            // 2. [검증 1: 방장 권한]
            if (target_room.host_player_fd != client_fd)
            {
                std::cerr << "    -> [검증 실패] (FD: " << client_fd << ")는 방장이 아님!" << std::endl;
                buffer.erase(buffer.begin(), buffer.begin() + packet_length);
                continue; // (방장이 아니므로 요청 무시)
            }
            
            std::cout << "    -> [검증 1 통과] 방장(FD: " << client_fd << ")이 요청함." << std::endl;

            // 3. [검증 2: 모두 준비]
            bool all_ready = true;
            
            // 3-A: 유령 플레이어 제거 (게임 시작 전에 연결이 끊어진 플레이어를 제거)
            // [핵심 수정] 게임 시작 전에 유령 플레이어(연결이 끊어진 플레이어)를 제거
            // 연결이 끊어진 플레이어의 fd는 epoll에서 제거되지만, players 목록에는 남아있을 수 있음
            auto players_it = target_room.players.begin();
            while (players_it != target_room.players.end())
            {
                // 플레이어의 fd가 여전히 유효한지 확인 (player_room_map에 있는지 확인)
                // player_room_map에 없는 플레이어는 유령 플레이어로 간주
                if (player_room_map.find(players_it->fd) == player_room_map.end())
                {
                    std::cout << "    -> [유령 플레이어 제거] FD " << players_it->fd << "는 연결이 끊어졌지만 players 목록에 남아있었습니다. 제거합니다." << std::endl;
                    players_it = target_room.players.erase(players_it);
                }
                else
                {
                    ++players_it;
                }
            }
            
            // 3-B: 방에 있는 모든 플레이어가 ready 상태인지 확인
            // (최소 2명 이상이어야 하고, 모든 플레이어가 ready여야 함)
            if (target_room.players.size() < 2)
            {
                all_ready = false;
                std::cerr << "    -> [검증 2 실패] 플레이어가 " << target_room.players.size() 
                          << "명이므로 게임 시작 불가 (최소 2명 필요)." << std::endl;
            }
            else
            {
                // 3-B: 방에 있는 모든 플레이어가 ready 상태인지 확인
                for (const Player& p : target_room.players)
                {
                    // player_ready_states 맵에 이 플레이어가 있는지 확인
                    auto ready_it = target_room.player_ready_states.find(p.fd);
                    
                    if (ready_it == target_room.player_ready_states.end())
                    {
                        // 준비 상태가 등록되지 않음 (ready를 누르지 않음)
                        all_ready = false;
                        std::cerr << "    -> [검증 2 실패] (FD: " << p.fd << ")가 준비 상태가 아님." << std::endl;
                        break;
                    }
                    else if (ready_it->second == false)
                    {
                        // 준비 상태가 false
                        all_ready = false;
                        std::cerr << "    -> [검증 2 실패] (FD: " << p.fd << ")가 준비 안 함." << std::endl;
                        break;
                    }
                }
            }

            // 4. [최종 판정]
            if (all_ready)
            {
                // [성공!]
                std::cout << "    -> [최종 승인] M3 게임 시작! (ID 331 방송)" << std::endl;
                
                // [핵심 수정] 첫 턴 플레이어를 랜덤으로 선택
                int total_players = (int)target_room.players.size();
                int first_turn_index = rand() % total_players; // 0부터 (total_players - 1)까지 랜덤 선택
                
                // [핵심 수정] 선택된 플레이어의 인덱스를 current_turn_index에 설정
                target_room.current_turn_index = first_turn_index;
                int first_turn_player_id = target_room.players[first_turn_index].fd;
                
                std::cout << "    -> [첫 턴 선택] 랜덤으로 선택된 플레이어: 인덱스 " << first_turn_index 
                          << ", FD " << first_turn_player_id << std::endl;
                std::cout << "    -> [서버 상태 설정] current_turn_index = " << target_room.current_turn_index 
                          << ", current_turn_fd = " << first_turn_player_id << std::endl;

                // ID 331 페이로드 (int first_turn_player_id)
                response_payload.resize(sizeof(int));
                std::memcpy(response_payload.data(), &first_turn_player_id, sizeof(int));

                
                // '방 안의 모든 사람'에게 방송
                for (const Player& p : target_room.players)
                {
                    SendMessage(p.fd, 331, response_payload);
                }
                target_room.state = RoomState::INGAME;

                // 1. ID 430용 페이로드 조립 (int + int = 8바이트)
                std::vector<char> turn_payload(sizeof(int) * 2); 
                
                // 1-1. 페이로드 [0~3]: next_player_id (방금 구한 first_turn_player_id)
                std::memcpy(turn_payload.data(), &first_turn_player_id, sizeof(int));

                // 1-2. 페이로드 [4~7]: turn_time_limit_sec (30초)
                int turn_time = 30;
                std::memcpy(turn_payload.data() + sizeof(int), &turn_time, sizeof(int));

                // 2. '방 안의 모든 사람'에게 ID 430 방송
                std::cout << "   -> [M3] 첫 턴(FD: " << first_turn_player_id << ") 시작! (ID 430 방송)" << std::endl;
                for (const Player& p : target_room.players)
                {
                    SendMessage(p.fd, 430, turn_payload);
                }
                // ===============================================
                }
            }
            else
            {
                // [실패!]
                std::cerr << "    -> [최종 거부] '모두 준비' 상태가 아님. 요청 무시." << std::endl;
                // (방장에게 "아직 준비 안 됨"이라고 응답을 보내줘도 좋지만,
                // M2 규약상 필수는 아니므로 지금은 무시합니다.)
            }
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
            // ID 400은 서버가 브로드캐스트하는 것이므로 클라이언트 요청 핸들러가 없어야 함
            // 클라이언트는 ID 500을 보내고, 서버는 ID 400을 브로드캐스트함
            // 이 핸들러는 제거됨 (ID 500 핸들러로 대체)
        else if (message_id == 410) // C2S_PlayerAim (Payload: float angle)
        {
            std::cout << "  -> ID 410 (조준) 요청 받음! (FD: " << client_fd << ")" << std::endl;

            // 1. '어느 방'에서 보냈는지 찾기
            auto map_it = player_room_map.find(client_fd);
            if (map_it == player_room_map.end())
            {
                std::cerr << "    -> 오류: 방에 속하지 않은 유저가 조준 요청." << std::endl;
                continue; 
            }
            int room_id = map_it->second;
            Room& target_room = global_rooms[room_id];

            // 2. [검증] 'INGAME' 상태가 맞는지?
            if (target_room.state == RoomState::INGAME)
            {
                // ===============================================
                // [M3 3단계] ID 410 로직
                // ===============================================
                
                // 1. ID 410의 페이로드(float) 읽기
                float angle = get_float_le(payload_data);

                std::cout << "    -> [INGAME] ID 410 처리. (FD: " << client_fd 
                          << ", Angle: " << angle << "). ID 411 방송 시작." << std::endl;

                // 2. ID 411 (방송용) 페이로드 조립 (int + float = 8 바이트)
                std::vector<char> notify_payload(sizeof(int) + sizeof(float));
                
                // 2-1. [0~3]: player_id (int) - (요청 보낸 사람)
                std::memcpy(notify_payload.data(), &client_fd, sizeof(int));
                
                // 2-2. [4~7]: angle (float) - (받은 값을 그대로 씁니다)
                std::memcpy(notify_payload.data() + sizeof(int), &angle, sizeof(float));
                
                // 3. '방 안의 모든 사람'에게 방송
                for (const Player& p : target_room.players)
                {
                    SendMessage(p.fd, 411, response_payload);
                }
                // ===============================================
            }
            else
            {
                // (LOBBY 상태에서 조준 요청을 보냄! -> 무시)
                std::cerr << "    -> [상태 오류] LOBBY 상태에서 ID 410 수신. 무시." << std::endl;
            }
        }
        else if (message_id == 420) // C2S_PlayerFire (Payload: float power)
        {
            std::cout << "  -> ID 420 (발사) 요청 받음! (FD: " << client_fd << ")" << std::endl;

            // 1. '어느 방'에서 보냈는지 찾기
            auto map_it = player_room_map.find(client_fd);
            if (map_it == player_room_map.end())
            {
                std::cerr << "     -> 오류: 방에 속하지 않은 유저가 발사 요청." << std::endl;
                continue; 
            }
            int room_id = map_it->second;
            Room& target_room = global_rooms[room_id];

            // 2. [검증 1] 'INGAME' 상태가 맞는지?
            if (target_room.state == RoomState::INGAME)
            {
                // ===============================================
                // [M3 3단계] '턴 권한' 검증
                // ===============================================
                
                // 2-A. '현재 턴인 플레이어'의 FD를 가져옵니다. (M3 4단계 수정)
                int current_turn_fd = target_room.players[target_room.current_turn_index].fd;

                // 2-B. '현재 턴인 사람'과 '요청 보낸 사람'이 같은지 비교
                if (client_fd == current_turn_fd)
                {
                     // [검증 통과!]
                     std::cout << "     -> [권한 통과] (FD: " << client_fd << ")가 발사!" << std::endl;
                     
                     // ========================================================
                     // [M3 4단계] '발사 판정' 및 'HP 깎기' 로직이 여기에 들어옵니다.
                     // (우리가 멈췄던 지점입니다. 곧 여기를 채울 겁니다!)
                     // ========================================================
                     // 1. [판정] 쏜 사람(Shooter)과 맞은 사람(Victim)을 결정합니다.
                    int shooter_id = client_fd; // 쏜 사람 = 요청 보낸 사람

                    // 2. '다음 턴' 인덱스를 '나머지(%)' 연산으로 구합니다. (순환)
                    int total_players = (int)target_room.players.size();
                    int victim_index = (target_room.current_turn_index + 1) % total_players;

                    // 3. 'players' 벡터에서 '맞은 사람' 객체의 '참조(&)'를 가져옵니다.
                    Player& victim = target_room.players[victim_index];
                    int victim_id = victim.fd;

                    // 4. [HP 깎기] 고정 대미지를 30으로 정하고, 피해자의 HP를 깎습니다.
                    int damage = 30;
                    victim.hp -= damage;
                    
                    // (HP가 0보다 낮아지면 0으로 고정)
                    if (victim.hp < 0) {
                        victim.hp = 0;
                    }

                    std::cout << "     -> [판정] (FD: " << shooter_id << ")가 (FD: " << victim_id
                            << ")에게 " << damage << " 피해! (남은 HP: " << victim.hp << ")" << std::endl;
                    // ========================================================
                 // [M3 4단계] 'ID 421' (발사 결과) 방송
                 // ========================================================
                 
                 // 5. [ID 421 방송] 방금 일어난 '결과'를 모두에게 방송
                 // 페이로드 (int 4개 = 16 바이트)
                 std::vector<char> notify_payload(sizeof(int) * 4); 

                 // 5-1. [0~3]: shooter_id (int)
                 std::memcpy(notify_payload.data(), &shooter_id, sizeof(int));
                 
                 // 5-2. [4~7]: victim_id (int)
                 std::memcpy(notify_payload.data() + sizeof(int), &victim_id, sizeof(int));
                 
                 // 5-3. [8~11]: damage (int)
                 std::memcpy(notify_payload.data() + (sizeof(int) * 2), &damage, sizeof(int));
                 
                 // 5-4. [12~15]: victim_new_hp (int) - 방금 깎은 그 HP
                 std::memcpy(notify_payload.data() + (sizeof(int) * 3), &victim.hp, sizeof(int));

                 // 6. '방 안의 모든 사람'에게 ID 421 방송
                 for (const Player& p : target_room.players)
                 {
                     SendMessage(p.fd, 421, notify_payload);
                 }
                 // ========================================================
                 for (const Player& p : target_room.players)
                 {
                     SendMessage(p.fd, 421, notify_payload);
                 }

                 // ========================================================
                 // [M3 4단계] '사망 검사' (HP가 0이고, 아직 안 죽었다면)
                 // ========================================================
                 
                 // 7. [사망 검사]
                 if (victim.hp == 0 && victim.is_dead == false)
                 {
                     // 7-1. [상태 변경] '사망'으로 상태 변경 (중복 사망 방지)
                     victim.is_dead = true;
                     int killer_id = shooter_id; // (shooter_id는 위에서 구해둠)

                     std::cout << "     -> [사망!] (FD: " << victim_id << ")가 (FD: " << killer_id
                               << ")에게 사망했습니다! (ID 422 방송)" << std::endl;

                     // 7-2. [ID 422 방송] 페이로드 조립 (int + int = 8 바이트)
                     std::vector<char> death_payload(sizeof(int) * 2);
                     
                     // [0~3]: victim_id (int)
                     std::memcpy(death_payload.data(), &victim_id, sizeof(int));
                     
                     // [4~7]: killer_id (int)
                     std::memcpy(death_payload.data() + sizeof(int), &killer_id, sizeof(int));

                     // 7-3. '방 안의 모든 사람'에게 ID 422 방송
                     for (const Player& p : target_room.players)
                     {
                         SendMessage(p.fd, 422, death_payload);
                     }
                 }

                 // 8. [승리 검사] 방금 '사망'으로 인해 게임이 끝났는지 검사
                 int alive_count = 0;
                 int winner_id = 0; // '최후의 승자'의 FD

                 // 8-1. 'players' 벡터를 순회하며 '살아있는' 사람을 셉니다.
                 for (const Player& p : target_room.players)
                 {
                     if (p.is_dead == false)
                     {
                         alive_count++;
                         winner_id = p.fd; // '살아있는' 사람을 '승자' 후보로 저장
                     }
                 }

                 // 8-2. [판정] 살아있는 사람이 1명 이하라면 (0명 or 1명)
                 if (alive_count <= 1)
                 {
                     // [게임 종료!]
                     std::cout << "     -> [게임 종료!] 승자 (FD: " << winner_id
                               << ") 발생! (ID 440 방송)" << std::endl;
                               
                     // 8-3. [ID 440 방송] 페이로드 조립 (int winner_player_id)
                     std::vector<char> end_payload(sizeof(int));
                     std::memcpy(end_payload.data(), &winner_id, sizeof(int));

                     // 8-4. '방 안의 모든 사람'에게 ID 440 방송
                     for (const Player& p : target_room.players)
                     {
                         SendMessage(p.fd, 440, end_payload);
                     }
                     
                     // (이제 5단계: '로비 복귀' 로직이 여기에 이어집니다)
                    // ========================================================
                     // [M3 5단계] '로비 복귀'를 위한 서버 '방' 초기화
                     // ========================================================

                     // 1. 방 상태를 '로비'로 되돌림
                     target_room.state = RoomState::LOBBY;
                     std::cout << "     -> [초기화] " << room_id << "번 방을 'LOBBY' 상태로 되돌립니다." << std::endl;

                     // 2. '준비 상태' 맵을 '비워서' 모두 '준비 안 함' 상태로 만듦
                     target_room.player_ready_states.clear();

                     // 3. '모든' 플레이어의 HP와 사망 상태를 '초기화'
                     // (주의: const Player& p가 아니라 '수정'을 위해 Player& p를 사용!)
                     for (Player& p : target_room.players)
                     {
                         p.hp = 100;    // '기본값' HP
                         p.is_dead = false; // '기본값' 사망 상태
                     }
                     
                     // 4. 턴 인덱스 초기화 (선택 사항이지만, 깔끔합니다)
                     target_room.current_turn_index = 0;

                     // ========================================================
                     // ★★★ 아주 중요 ★★★
                     // 게임이 끝났으므로, '턴 넘기기' 로직을 실행하면 안 됩니다!
                     // HandleBuffer의 다음 'while(true)' 루프로 돌아갑니다.


                     continue; 
                 }
                 
                 // (만약 alive_count > 1 이라면, 이 'if'문을 건너뛰고
                 //  아래의 '턴 넘기기' 로직으로 자연스럽게 흘러갑니다.)
                 // 9. [턴 넘기기] (살아있는 사람이 2명 이상일 때만 실행됨)
                 total_players = (int)target_room.players.size();
                 int next_turn_index = target_room.current_turn_index; // 일단 현재 턴으로 시작

                 // 9-1. '살아있는' 다음 턴 플레이어를 찾을 때까지 '계속' 턴을 넘김
                 // (최악의 경우 1바퀴 돌아 자기 자신에게 돌아옴 = 버그 없음)
                 while (true)
                 {
                     // (1) 다음 후보 인덱스 계산
                     next_turn_index = (next_turn_index + 1) % total_players;
                     
                     // (2) 그 후보가 '살아있다면'
                     if (target_room.players[next_turn_index].is_dead == false)
                     {
                         break; // '후보' 확정! 'while' 루프 탈출
                     }
                     // (죽었다면? -> while 루프가 다시 돌면서 +1 하여 그 다음 사람 검사)
                 }

                 // 9-2. [서버 상태 갱신] '방'의 현재 턴을 '새 인덱스'로 갱신
                 target_room.current_turn_index = next_turn_index;
                 
                 // 9-3. [ID 430 방송] 새 턴을 모두에게 알림 (ID 330에서 썼던 코드 재활용)
                 int next_player_fd = target_room.players[next_turn_index].fd;
                 int turn_time = 30; // 30초

                 std::cout << "     -> [턴 넘기기] 다음 턴 (FD: " << next_player_fd
                           << ") 시작! (ID 430 방송)" << std::endl;
                           
                 std::vector<char> turn_payload(sizeof(int) * 2);
                 std::memcpy(turn_payload.data(), &next_player_fd, sizeof(int));
                 std::memcpy(turn_payload.data() + sizeof(int), &turn_time, sizeof(int));
                 
                 for (const Player& p : target_room.players)
                 {
                     SendMessage(p.fd, 430, turn_payload);
                 }
                 // ========================================================

                }
                else
                {
                    // (내 턴도 아닌데 발사 요청함! -> 해킹/버그)
                    std::cerr << "     -> [권한 오류] (FD: " << client_fd 
                              << ")가 턴(FD: " << current_turn_fd << ")이 아닐 때 ID 420 보냄. 무시." << std::endl;
                }
            }
            else
            {
                std::cerr << "     -> [상태 오류] LOBBY 상태에서 ID 420 수신. 무시." << std::endl;
            }
        }
	else if (message_id == 500) // C2S_MoveRequest (Payload: int player_id + float x, y, z + float rot x, y, z, w)
        {
            std::cout << "  -> ID 500 (이동 요청) 받음! (FD: " << client_fd << ")" << std::endl;
            
            // 1. '어느 방'에서 보냈는지 찾기
            auto map_it = player_room_map.find(client_fd);
            if (map_it == player_room_map.end())
            {
                std::cerr << "     -> 오류: 방에 속하지 않은 유저가 이동 요청." << std::endl;
                buffer.erase(buffer.begin(), buffer.begin() + packet_length);
                continue; 
            }
            
            int room_id = map_it->second;
            Room& target_room = global_rooms[room_id];
            
            // 2. [검증] 'INGAME' 상태가 맞는지?
            if (target_room.state == RoomState::INGAME)
            {
                // 3. 페이로드 파싱
                // Payload 구조: int player_id(4) + float x(4) + float y(4) + float z(4) + float rot_x(4) + float rot_y(4) + float rot_z(4) + float rot_w(4) = 32바이트
                int player_id;
                std::memcpy(&player_id, payload_data, sizeof(int));
                
                float pos_x = get_float_le(payload_data + sizeof(int));
                float pos_y = get_float_le(payload_data + sizeof(int) + sizeof(float));
                float pos_z = get_float_le(payload_data + sizeof(int) + sizeof(float) * 2);
                
                float rot_x = get_float_le(payload_data + sizeof(int) + sizeof(float) * 3);
                float rot_y = get_float_le(payload_data + sizeof(int) + sizeof(float) * 4);
                float rot_z = get_float_le(payload_data + sizeof(int) + sizeof(float) * 5);
                float rot_w = get_float_le(payload_data + sizeof(int) + sizeof(float) * 6);
                
                std::cout << "     -> [INGAME] ID 500 처리. (FD: " << client_fd 
                          << ", PlayerID: " << player_id << ", Pos: (" << pos_x << ", " << pos_y << ", " << pos_z << "))" << std::endl;
                
                // 4. ID 400 (PlayerMove Notify) 브로드캐스트
                // ID 400 페이로드: int player_id(4) + float x(4) + float y(4) + float z(4) + float rot_x(4) + float rot_y(4) + float rot_z(4) + float rot_w(4) = 32바이트
                // 클라이언트가 보낸 player_id를 그대로 사용해야 함
                std::vector<char> notify_payload(sizeof(int) + sizeof(float) * 7);
                
                int player_id_for_notify = player_id; // 클라이언트가 보낸 player_id 사용
                std::memcpy(notify_payload.data(), &player_id_for_notify, sizeof(int));
                std::memcpy(notify_payload.data() + sizeof(int), &pos_x, sizeof(float));
                std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float), &pos_y, sizeof(float));
                std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 2, &pos_z, sizeof(float));
                std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 3, &rot_x, sizeof(float));
                std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 4, &rot_y, sizeof(float));
                std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 5, &rot_z, sizeof(float));
                std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 6, &rot_w, sizeof(float));
                
                // 5. '방 안의 모든 사람'에게 ID 400 방송 (요청을 보낸 클라이언트는 제외)
                // [핵심 수정] 요청을 보낸 클라이언트는 자신의 이동 정보를 이미 알고 있으므로 불필요한 패킷 전송을 방지
                for (const Player& p : target_room.players)
                {
                    // 요청을 보낸 클라이언트는 제외
                    if (p.fd != client_fd)
                    {
                        SendMessage(p.fd, 400, notify_payload);
                        std::cout << "     -> [ID 400 전송] FD " << p.fd << "에게 PlayerMove Notify 전송" << std::endl;
                    }
                    else
                    {
                        std::cout << "     -> [ID 400 건너뜀] 요청을 보낸 클라이언트(FD: " << client_fd << ")는 제외" << std::endl;
                    }
                }
            }
            else
            {
                std::cerr << "     -> [상태 오류] LOBBY 상태에서 ID 500 수신. 무시." << std::endl;
            }
            
            // 버퍼 정리
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        
       else if (message_id == 600) // C2S_PlayerFire (Payload: int player_id, float firePoint_x, float firePoint_y, float firePoint_z, float fireRotation_x, float fireRotation_y, float fireRotation_z, float fireRotation_w)
{
    std::cout << " -> ID 600 (발사 요청) 받음! (FD: " << client_fd << ")" << std::endl;
    
    // 1. '어느 방'에서 보냈는지 찾기
    auto map_it = player_room_map.find(client_fd);
    if (map_it == player_room_map.end())
    {
        std::cerr << " -> 오류: 방에 속하지 않은 유저가 발사 요청." << std::endl;
        buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        continue;
    }
    
    int room_id = map_it->second;
    Room& target_room = global_rooms[room_id];
    
    // 2. [검증 1] 'INGAME' 상태가 맞는지?
    if (target_room.state != RoomState::INGAME)
    {
        std::cerr << " -> [상태 오류] LOBBY 상태에서 ID 600 수신. 무시." << std::endl;
        buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        continue;
    }
    
    // 2-1. [핵심 수정] 게임 중에도 유령 플레이어 제거 (발사 요청 처리 전에 정리)
    // 연결이 끊어진 플레이어의 fd는 epoll에서 제거되지만, players 목록에는 남아있을 수 있음
    size_t players_before_cleanup = target_room.players.size();
    auto players_it = target_room.players.begin();
    while (players_it != target_room.players.end())
    {
        // 플레이어의 fd가 여전히 유효한지 확인 (player_room_map에 있는지 확인)
        // player_room_map에 없는 플레이어는 유령 플레이어로 간주
        if (player_room_map.find(players_it->fd) == player_room_map.end())
        {
            std::cout << " -> [유령 플레이어 제거] FD " << players_it->fd << "는 연결이 끊어졌지만 players 목록에 남아있었습니다. 제거합니다." << std::endl;
            // [중요] 현재 턴 인덱스가 제거되는 플레이어보다 뒤에 있으면 인덱스를 조정해야 함
            int removed_index = std::distance(target_room.players.begin(), players_it);
            if (removed_index < target_room.current_turn_index && target_room.current_turn_index > 0)
            {
                target_room.current_turn_index--;
                std::cout << " -> [턴 인덱스 조정] 유령 플레이어 제거로 인해 current_turn_index를 " << target_room.current_turn_index << "로 조정" << std::endl;
            }
            else if (removed_index == target_room.current_turn_index)
            {
                // [핵심 수정] 현재 턴 플레이어가 유령 플레이어인 경우, 다음 유효한 플레이어로 턴 전환
                std::cerr << " -> [치명적 오류] 현재 턴 플레이어(FD: " << players_it->fd << ")가 유령 플레이어입니다! 즉시 다음 유효한 플레이어로 턴 전환합니다." << std::endl;
                players_it = target_room.players.erase(players_it);
                // 다음 유효한 플레이어 찾기
                if (!target_room.players.empty())
                {
                    int next_valid_index = 0;
                    for (size_t i = 0; i < target_room.players.size(); i++)
                    {
                        if (player_room_map.find(target_room.players[i].fd) != player_room_map.end() && 
                            !target_room.players[i].is_dead)
                        {
                            next_valid_index = i;
                            break;
                        }
                    }
                    target_room.current_turn_index = next_valid_index;
                    std::cout << " -> [턴 인덱스 강제 조정] 다음 유효한 플레이어로 턴 전환: 인덱스 " << target_room.current_turn_index << ", FD " << target_room.players[target_room.current_turn_index].fd << std::endl;
                }
                continue; // erase 후에는 players_it가 이미 다음을 가리키므로 continue
            }
            players_it = target_room.players.erase(players_it);
        }
        else
        {
            ++players_it;
        }
    }
    
    // [핵심 수정] 유령 플레이어 제거 후 current_turn_index가 유효한 범위인지 확인
    if (!target_room.players.empty() && target_room.current_turn_index >= (int)target_room.players.size())
    {
        std::cerr << " -> [경고] current_turn_index(" << target_room.current_turn_index << ")가 유효 범위를 벗어났습니다. 0으로 재설정합니다." << std::endl;
        target_room.current_turn_index = 0;
    }
    
    if (players_before_cleanup != target_room.players.size())
    {
        std::cout << " -> [유령 플레이어 제거 완료] 제거 전: " << players_before_cleanup << "명, 제거 후: " << target_room.players.size() << "명" << std::endl;
    }
    
    // 3. ID 600 페이로드 파싱 (먼저 player_id를 읽어서 턴 검증에 사용)
    // Payload: int player_id(4) + float firePoint(12) + float fireRotation(16) = 32바이트
    int player_id;
    float firePoint_x, firePoint_y, firePoint_z;
    float fireRotation_x, fireRotation_y, fireRotation_z, fireRotation_w;
    
    std::memcpy(&player_id, payload_data, sizeof(int));
    std::memcpy(&firePoint_x, payload_data + sizeof(int), sizeof(float));
    std::memcpy(&firePoint_y, payload_data + sizeof(int) + sizeof(float), sizeof(float));
    std::memcpy(&firePoint_z, payload_data + sizeof(int) + sizeof(float) * 2, sizeof(float));
    std::memcpy(&fireRotation_x, payload_data + sizeof(int) + sizeof(float) * 3, sizeof(float));
    std::memcpy(&fireRotation_y, payload_data + sizeof(int) + sizeof(float) * 4, sizeof(float));
    std::memcpy(&fireRotation_z, payload_data + sizeof(int) + sizeof(float) * 5, sizeof(float));
    std::memcpy(&fireRotation_w, payload_data + sizeof(int) + sizeof(float) * 6, sizeof(float));
    
    // 3-1. [검증 2] '턴 권한' 검증 (클라이언트가 보낸 player_id로 검증)
    // [핵심 수정] current_turn_index가 유효한 범위인지 확인
    if (target_room.current_turn_index < 0 || target_room.current_turn_index >= (int)target_room.players.size())
    {
        std::cerr << " -> [치명적 오류] current_turn_index(" << target_room.current_turn_index << ")가 유효 범위를 벗어났습니다! (players.size(): " << target_room.players.size() << ")" << std::endl;
        // 첫 번째 유효한 플레이어로 재설정
        if (!target_room.players.empty())
        {
            target_room.current_turn_index = 0;
            std::cerr << " -> [복구] current_turn_index를 0으로 재설정했습니다." << std::endl;
        }
        else
        {
            std::cerr << " -> [치명적 오류] 플레이어가 없습니다! 발사 요청을 무시합니다." << std::endl;
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
            continue;
        }
    }
    
    int current_turn_fd = target_room.players[target_room.current_turn_index].fd;
    std::cout << " -> [턴 검증] current_turn_index: " << target_room.current_turn_index << std::endl;
    std::cout << " -> [턴 검증] players.size(): " << target_room.players.size() << std::endl;
    std::cout << " -> [턴 검증] 현재 턴 FD: " << current_turn_fd << ", 요청 FD: " << client_fd << ", PlayerID: " << player_id << std::endl;
    
    // 클라이언트가 보낸 player_id가 현재 턴인지 확인 (player_id는 UserID로 사용되며, 서버에서는 fd와 일치)
    if (player_id != current_turn_fd)
    {
        std::cerr << " -> [권한 오류] PlayerID " << player_id << "는 현재 턴이 아님. (현재 턴: FD " << current_turn_fd << ")" << std::endl;
        buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        continue;
    }
    
    std::cout << " -> [INGAME] ID 600 처리. (FD: " << client_fd << ", PlayerID: " << player_id << "). ID 420 방송 시작." << std::endl;
    
    // 5. ID 420 (PlayerFire Notify) 페이로드 조립 및 방송
    // ID 420 페이로드: int player_id(4) + float firePoint(12) + float fireRotation(16) = 32바이트
    // 클라이언트가 보낸 player_id를 그대로 사용해야 함
    int player_id_for_notify = player_id; // 클라이언트가 보낸 player_id 사용
    std::vector<char> notify_payload(sizeof(int) + sizeof(float) * 7);
    std::memcpy(notify_payload.data(), &player_id_for_notify, sizeof(int));
    std::memcpy(notify_payload.data() + sizeof(int), &firePoint_x, sizeof(float));
    std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float), &firePoint_y, sizeof(float));
    std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 2, &firePoint_z, sizeof(float));
    std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 3, &fireRotation_x, sizeof(float));
    std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 4, &fireRotation_y, sizeof(float));
    std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 5, &fireRotation_z, sizeof(float));
    std::memcpy(notify_payload.data() + sizeof(int) + sizeof(float) * 6, &fireRotation_w, sizeof(float));
    
    // 6. '방 안의 모든 사람'에게 ID 420 방송
    std::cout << " -> [ID 420 방송] 모든 플레이어에게 발사 알림 전송" << std::endl;
    for (const Player& p : target_room.players)
    {
        SendMessage(p.fd, 420, notify_payload);
    }
    
    // 7. [발사 판정] 쏜 사람과 맞은 사람 결정
    // [핵심 수정] Shooter ID는 클라이언트가 보낸 player_id가 아닌, 실제 요청을 보낸 client_fd를 사용
    // 턴 검증을 통과했으므로 current_turn_fd == client_fd가 보장됨
    // 이렇게 하면 FD와 PlayerID의 일관성이 보장됨
    int shooter_id = current_turn_fd; // 실제 요청을 보낸 FD 사용 (player_id는 신뢰할 수 없음)
    int total_players = (int)target_room.players.size();
    int current_turn_index = target_room.current_turn_index;
    
    std::cout << " -> [Shooter 결정] Shooter ID: " << shooter_id << " (FD: " << client_fd << ", current_turn_fd: " << current_turn_fd << ", 클라이언트가 보낸 player_id: " << player_id << ")" << std::endl;
    
    // [핵심 수정] goto를 사용하기 전에 모든 변수를 미리 선언 (C++ 컴파일 오류 방지)
    // 턴 전환에 필요한 변수들을 미리 선언 (goto를 위해)
    int next_turn_index = current_turn_index;
    int search_count = 0;
    bool found_next_turn = false;
    int alive_count = 0;
    int winner_id = 0;
    
    // 데미지 처리에 필요한 변수들을 미리 선언 (goto를 위해)
    int victim_id = 0;
    int damage = 0;
    std::vector<char> fire_result_payload;
    int victim_index = -1;
    
    // [핵심 수정] current_turn_index가 유효한 범위인지 확인
    if (current_turn_index < 0 || current_turn_index >= total_players)
    {
        std::cerr << " -> [치명적 오류] current_turn_index(" << current_turn_index << ")가 유효 범위를 벗어났습니다! (players.size(): " << total_players << ")" << std::endl;
        // 첫 번째 유효한 플레이어로 재설정
        if (!target_room.players.empty())
        {
            current_turn_index = 0;
            target_room.current_turn_index = 0;
            std::cerr << " -> [복구] current_turn_index를 0으로 재설정했습니다." << std::endl;
        }
        else
        {
            std::cerr << " -> [치명적 오류] 플레이어가 없습니다! 턴 전환만 진행합니다." << std::endl;
            goto turn_transition;
        }
    }
    
    std::cout << " -> [Victim 선택] 현재 턴 인덱스: " << current_turn_index << ", 현재 턴 FD: " << target_room.players[current_turn_index].fd << ", Shooter ID: " << shooter_id << ", 총 플레이어 수: " << total_players << std::endl;
    
    // 현재 턴 플레이어를 제외한 다른 살아있는 플레이어를 victim으로 선택
    // [핵심 수정] 유령 플레이어를 제외하고 유효한 플레이어만 고려
    victim_index = -1; // 이미 위에서 선언됨
    for (int i = 0; i < total_players; i++)
    {
        int candidate_index = (current_turn_index + 1 + i) % total_players;
        int candidate_fd = target_room.players[candidate_index].fd;
        
        // [핵심 수정] 유효한 플레이어인지 확인 (player_room_map에 있는 플레이어만 유효)
        // 유령 플레이어는 player_room_map에 없으므로 제외됨
        bool is_valid_player = (player_room_map.find(candidate_fd) != player_room_map.end());
        
        // [핵심 수정] 현재 턴 플레이어(Shooter)를 명시적으로 제외
        bool is_shooter = (candidate_fd == shooter_id);
        
        std::cout << " -> [Victim 선택 후보 " << (i + 1) << "] 인덱스: " << candidate_index << ", FD: " << candidate_fd << ", is_dead: " << (target_room.players[candidate_index].is_dead ? "true" : "false") << ", is_valid: " << (is_valid_player ? "true" : "false") << ", is_shooter: " << (is_shooter ? "true" : "false") << std::endl;
        
        if (!is_shooter && 
            candidate_index != current_turn_index && 
            target_room.players[candidate_index].is_dead == false &&
            is_valid_player)
        {
            victim_index = candidate_index;
            std::cout << " -> [Victim 선택] ✅ Victim 찾음: 인덱스 " << victim_index << ", FD " << target_room.players[victim_index].fd << " (유효한 플레이어, Shooter 아님)" << std::endl;
            break;
        }
        else if (is_shooter)
        {
            std::cout << " -> [Victim 선택] ❌ 인덱스 " << candidate_index << ", FD " << candidate_fd << "는 Shooter이므로 제외" << std::endl;
        }
        else if (!is_valid_player)
        {
            std::cout << " -> [Victim 선택] ❌ 인덱스 " << candidate_index << ", FD " << candidate_fd << "는 유령 플레이어이므로 제외" << std::endl;
        }
    }
    
    if (victim_index == -1)
    {
        std::cerr << " -> [경고] Victim을 찾을 수 없습니다! (현재 턴 인덱스: " << current_turn_index << ")" << std::endl;
        std::cerr << " -> [경고] 데미지 처리를 건너뛰고 턴 전환만 진행합니다." << std::endl;
        // Victim을 찾지 못했어도 턴 전환은 진행해야 함 (포탄 발사 후 무조건 턴 전환)
        // ID 421과 ID 422는 건너뛰고 바로 턴 전환으로 이동
        goto turn_transition;
    }
    
    // victim 참조 변수를 사용하는 코드를 별도 스코프로 분리 (goto를 위해)
    {
        Player& victim = target_room.players[victim_index];
        victim_id = victim.fd; // victim의 fd는 UserID로 사용됨 (ID 331에서 전송됨)
        
        // Shooter와 Victim이 같으면 안 됨
        // [핵심 수정] shooter_id는 이제 current_turn_fd로 설정되므로, victim_id와 비교 시 일관성이 보장됨
        std::cout << " -> [Shooter/Victim 검증] Shooter ID: " << shooter_id << " (FD: " << shooter_id << "), Victim ID: " << victim_id << " (FD: " << victim_id << ")" << std::endl;
        if (shooter_id == victim_id)
        {
            std::cerr << " -> [경고] Shooter와 Victim이 같습니다! (PlayerID: " << shooter_id << ")" << std::endl;
            std::cerr << " -> [경고] 이는 현재 턴 플레이어가 자기 자신을 Victim으로 선택한 경우입니다." << std::endl;
            std::cerr << " -> [경고] 데미지 처리를 건너뛰고 턴 전환만 진행합니다." << std::endl;
            // Shooter와 Victim이 같아도 턴 전환은 진행해야 함 (포탄 발사 후 무조건 턴 전환)
            goto turn_transition;
        }
        
        // 8. [HP 깎기] 고정 대미지 30
        damage = 30;
        victim.hp -= damage;
        if (victim.hp < 0)
        {
            victim.hp = 0;
        }
        
        std::cout << " -> [판정] (PlayerID: " << shooter_id << ", FD: " << client_fd << ")가 (FD: " << victim_id << ")에게 " << damage << " 피해! (남은 HP: " << victim.hp << ")" << std::endl;
        
        // 9. ID 421 (Fire Result Notify) 방송
        fire_result_payload.resize(sizeof(int) * 4);
        std::memcpy(fire_result_payload.data(), &shooter_id, sizeof(int));
        std::memcpy(fire_result_payload.data() + sizeof(int), &victim_id, sizeof(int));
        std::memcpy(fire_result_payload.data() + sizeof(int) * 2, &damage, sizeof(int));
        std::memcpy(fire_result_payload.data() + sizeof(int) * 3, &victim.hp, sizeof(int));
        
        std::cout << " -> [ID 421 방송] 모든 플레이어에게 발사 결과 전송" << std::endl;
        for (const Player& p : target_room.players)
        {
            SendMessage(p.fd, 421, fire_result_payload);
        }
        
        // 10. [사망 검사]
        if (victim.hp == 0 && victim.is_dead == false)
        {
            victim.is_dead = true;
            int killer_id = shooter_id; // shooter_id는 이미 player_id로 설정됨
            
            std::cout << " -> [사망!] (FD: " << victim_id << ")가 (PlayerID: " << killer_id << ")에게 사망했습니다! (ID 422 방송)" << std::endl;
            
            std::vector<char> death_payload(sizeof(int) * 2);
            std::memcpy(death_payload.data(), &victim_id, sizeof(int));
            std::memcpy(death_payload.data() + sizeof(int), &killer_id, sizeof(int));
            
            std::cout << " -> [ID 422 방송] 모든 플레이어에게 사망 알림 전송" << std::endl;
            for (const Player& p : target_room.players)
            {
                SendMessage(p.fd, 422, death_payload);
            }
        }
    }
    
    // 11. [승리 검사] (변수는 이미 위에서 선언됨)
    alive_count = 0;
    winner_id = 0;
    for (const Player& p : target_room.players)
    {
        if (p.is_dead == false)
        {
            alive_count++;
            winner_id = p.fd;
        }
    }
    
    if (alive_count <= 1)
    {
        std::cout << " -> [게임 종료!] 승자 (FD: " << winner_id << ") 발생! (ID 440 방송)" << std::endl;
        
        std::vector<char> end_payload(sizeof(int));
        std::memcpy(end_payload.data(), &winner_id, sizeof(int));
        
        std::cout << " -> [ID 440 방송] 모든 플레이어에게 게임 종료 알림 전송" << std::endl;
        for (const Player& p : target_room.players)
        {
            SendMessage(p.fd, 440, end_payload);
        }
        
        // 로비 복귀
        target_room.state = RoomState::LOBBY;
        target_room.player_ready_states.clear();
        target_room.current_turn_index = 0;
        
        // 게임 종료 시 버퍼 정리 후 종료
        buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        continue;
    }
    
    // 12. [턴 전환] 살아있는 다음 플레이어 찾기 (포탄 발사 후 무조건 턴 전환)
turn_transition:
    // 변수들은 이미 위에서 선언되었으므로 재사용
    next_turn_index = current_turn_index;
    search_count = 0;
    found_next_turn = false;
    
    std::cout << " -> [턴 전환 시작] 현재 턴 인덱스: " << current_turn_index << ", 현재 턴 FD: " << target_room.players[current_turn_index].fd << ", 총 플레이어 수: " << total_players << std::endl;
    
    // 살아있는 다음 플레이어를 찾을 때까지 반복 (무한 루프 방지)
    // 최대 total_players번 검색 (모든 플레이어를 한 바퀴 돌기)
    // [핵심 수정] 유령 플레이어를 제외하고 유효한 플레이어만 고려
    while (search_count < total_players)
    {
        next_turn_index = (next_turn_index + 1) % total_players;
        search_count++;
        
        // [핵심 수정] 유효한 플레이어인지 확인 (player_room_map에 있는 플레이어만 유효)
        // 유령 플레이어는 player_room_map에 없으므로 제외됨
        bool is_valid_player = (player_room_map.find(target_room.players[next_turn_index].fd) != player_room_map.end());
        
        std::cout << " -> [턴 전환 검색 " << search_count << "/" << total_players << "] 인덱스: " << next_turn_index << ", FD: " << target_room.players[next_turn_index].fd << ", is_dead: " << (target_room.players[next_turn_index].is_dead ? "true" : "false") << ", is_valid: " << (is_valid_player ? "true" : "false") << std::endl;
        
        // 살아있고 유효한 플레이어를 찾으면 종료 (현재 턴 플레이어와 달라야 함)
        if (target_room.players[next_turn_index].is_dead == false && 
            next_turn_index != current_turn_index &&
            is_valid_player)
        {
            found_next_turn = true;
            std::cout << " -> [턴 전환 발견] 다음 턴 플레이어 찾음: 인덱스 " << next_turn_index << ", FD " << target_room.players[next_turn_index].fd << " (유효한 플레이어)" << std::endl;
            break;
        }
        else if (!is_valid_player)
        {
            std::cout << " -> [턴 전환 검색] 인덱스 " << next_turn_index << ", FD " << target_room.players[next_turn_index].fd << "는 유령 플레이어이므로 제외" << std::endl;
        }
    }
    
    // 모든 플레이어가 죽었거나 찾지 못한 경우 (이미 게임 종료 체크를 했으므로 발생하지 않아야 함)
    if (!found_next_turn)
    {
        std::cerr << " -> [오류] 살아있는 다음 플레이어를 찾을 수 없습니다! (검색 횟수: " << search_count << ", 현재 턴 인덱스: " << current_turn_index << ", 다음 턴 인덱스: " << next_turn_index << ")" << std::endl;
        std::cerr << " -> [오류] 게임 종료 체크를 이미 했으므로 살아있는 플레이어가 있어야 합니다. 강제로 다음 플레이어를 찾습니다." << std::endl;
        
        // 강제로 다음 살아있는 플레이어 찾기 (게임 종료 체크를 이미 했으므로 반드시 찾아야 함)
        for (int i = 0; i < total_players; i++)
        {
            int candidate_index = (current_turn_index + 1 + i) % total_players;
            if (candidate_index != current_turn_index && target_room.players[candidate_index].is_dead == false)
            {
                next_turn_index = candidate_index;
                found_next_turn = true;
                std::cout << " -> [강제 턴 전환] 다음 플레이어 찾음: 인덱스 " << next_turn_index << ", FD " << target_room.players[next_turn_index].fd << std::endl;
                break;
            }
        }
        
        // 여전히 찾지 못한 경우 (이론적으로 불가능하지만 안전장치)
        if (!found_next_turn)
        {
            std::cerr << " -> [치명적 오류] 살아있는 다음 플레이어를 찾을 수 없습니다! 턴 전환을 건너뜁니다." << std::endl;
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
            continue;
        }
    }
    
    // 턴 인덱스 업데이트 (found_next_turn이 true인 경우에만 실행)
    target_room.current_turn_index = next_turn_index;
    int next_turn_fd = target_room.players[next_turn_index].fd;
    
    std::cout << " -> [턴 전환 완료] 이전 턴: 인덱스 " << current_turn_index << " (FD " << target_room.players[current_turn_index].fd << "), 다음 턴: 인덱스 " << next_turn_index << " (FD " << next_turn_fd << ") (ID 430 방송)" << std::endl;
    
    // 13. ID 430 (Turn Start Notify) 방송 (무조건 실행)
    std::vector<char> turn_payload(sizeof(int) * 2);
    std::memcpy(turn_payload.data(), &next_turn_fd, sizeof(int));
    int turn_time = 30;
    std::memcpy(turn_payload.data() + sizeof(int), &turn_time, sizeof(int));
    
    std::cout << " -> [ID 430 방송] 모든 플레이어에게 턴 시작 알림 전송 (NextPlayerID: " << next_turn_fd << ")" << std::endl;
    for (const Player& p : target_room.players)
    {
        SendMessage(p.fd, 430, turn_payload);
        std::cout << "   -> [ID 430 전송] FD " << p.fd << "에게 전송 완료" << std::endl;
    }
    std::cout << " -> [ID 430 방송 완료] 총 " << target_room.players.size() << "명에게 전송" << std::endl;
    
    // 14. 버퍼 정리 (매우 중요!)
    buffer.erase(buffer.begin(), buffer.begin() + packet_length);
    }
    else
    {
        std::cout << "  -> 알 수 없는 ID (" << message_id << ") 입니다. 무시합니다." << std::endl;
        // 알 수 없는 ID의 경우에도 버퍼 정리
        if (packet_length > 0 && packet_length <= buffer.size())
        {
            buffer.erase(buffer.begin(), buffer.begin() + packet_length);
        }
        else
        {
            // 잘못된 패킷 길이면 버퍼 정리
            std::cerr << "[오류] 잘못된 패킷 길이로 인한 버퍼 정리: " << packet_length << " 바이트" << std::endl;
            buffer.clear();
            return;
        }
    }
    
    // 버퍼 크기가 비정상적으로 크면 경고
    if (buffer.size() > 1024 * 1024)  // 1MB 이상
    {
        std::cerr << "[경고] 버퍼 크기가 비정상적으로 큼: " << buffer.size() << " 바이트. 버퍼 정리." << std::endl;
        buffer.clear();
        return;
    }
    
    std::cout << "[메시지 처리 완료] 가방에 " << buffer.size() << " 바이트 남음." << std::endl;
    } // while 루프 종료
} // HandleBuffer 함수 종료


// ===============================================
// [!!] main 함수 시작
// ===============================================
int main(int argc, char* argv[])
{
    // 환경 변수에서 포트 읽기 (Railway/Render 호환)
    const char* env_port = std::getenv("PORT");
    const char* disable_ws = std::getenv("DISABLE_WEBSOCKET");  // WebSocket 비활성화 옵션
    int tcp_port = 7777;  // 기본값
    int ws_port = 7778;   // 기본값
    bool enable_websocket = true;
    
    // 명령줄 인자가 있으면 우선 사용
    if (argc >= 2) {
        tcp_port = atoi(argv[1]);
    } else if (env_port) {
        // Railway/Render는 하나의 포트만 제공
        // HTTP 서비스의 경우 WebSocket을 같은 포트에서 실행
        tcp_port = atoi(env_port);
        ws_port = tcp_port;  // WebSocket도 같은 포트 사용
        enable_websocket = true;  // WebSocket 활성화 (HTTP 업그레이드 지원)
        std::cout << "[배포 모드] 환경 변수 PORT=" << tcp_port << " 사용, WebSocket도 같은 포트에서 실행" << std::endl;
    }
    
    if (argc >= 3) {
        ws_port = atoi(argv[2]);
        enable_websocket = true;
    } else if (argc == 1 && !env_port) {
        // 인자도 없고 환경 변수도 없으면 기본값 사용
        std::cout << "포트 인자가 없어 기본값 사용: TCP=" << tcp_port << ", WebSocket=" << ws_port << std::endl;
    }
    
    // DISABLE_WEBSOCKET 환경 변수로 강제 비활성화 가능
    if (disable_ws && (std::string(disable_ws) == "1" || std::string(disable_ws) == "true")) {
        enable_websocket = false;
        std::cout << "[배포 모드] DISABLE_WEBSOCKET 환경 변수로 WebSocket 비활성화" << std::endl;
    }

    // Railway HTTP 서비스 모드: WebSocket만 사용 (HTTP 업그레이드 지원)
    bool is_railway_http = (env_port != nullptr && argc < 2);
    
    if (is_railway_http) {
        // Railway HTTP 서비스: WebSocket 서버만 시작 (HTTP 요청을 WebSocket으로 업그레이드)
        std::cout << "[Railway HTTP 모드] WebSocket 서버 시작 (포트: " << ws_port << ")" << std::endl;
        std::cout << "[Railway HTTP 모드] HTTP 요청을 WebSocket으로 업그레이드 처리" << std::endl;
        // cleanup 콜백을 람다로 감싸서 close_websocket_session=false로 호출
        ws_server = std::make_unique<WebSocketServer>(ws_port, HandleWebSocketMessage,
            [](int fd) { CleanupPlayer(fd, false); });
        ws_server->start();
        
        // WebSocket 서버가 HTTP 요청을 받아서 WebSocket으로 업그레이드하므로
        // TCP 서버는 시작하지 않음
        std::cout << "[Railway HTTP 모드] TCP 서버 비활성화" << std::endl;
        
        // WebSocket 서버가 종료될 때까지 대기
        std::cout << "[Railway HTTP 모드] 서버 실행 중..." << std::endl;
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    
    // 로컬 개발 모드: TCP와 WebSocket 모두 사용
    // WebSocket 서버 시작 (활성화된 경우만)
    if (enable_websocket) {
        // cleanup 콜백을 람다로 감싸서 close_websocket_session=false로 호출
        ws_server = std::make_unique<WebSocketServer>(ws_port, HandleWebSocketMessage,
            [](int fd) { CleanupPlayer(fd, false); });
        ws_server->start();
    } else {
        std::cout << "[배포 모드] WebSocket 서버 비활성화 (TCP 서버만 사용)" << std::endl;
    }

    // 1. socket, bind, listen
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { std::cerr << "소켓 생성 실패!" << std::endl; return 1; }
    std::cout << "서버 소켓 생성 성공 (FD: " << server_fd << ")" << std::endl;
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;           
    server_addr.sin_addr.s_addr = INADDR_ANY;   
    server_addr.sin_port = htons(tcp_port);         
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "바인딩 실패!" << std::endl; return 1;
    }
    std::cout << "바인딩 성공! 포트 " << tcp_port << " 할당." << std::endl;
    if (listen(server_fd, 5) < 0) { std::cerr << "listen 실패!" << std::endl; return 1; }
    std::cout << "listen 성공! 클라이언트 연결 대기 중..." << std::endl;

    // 2. epoll_create
    int epoll_fd = epoll_create(1); 
    if (epoll_fd < 0) { std::cerr << "epoll 생성 실패!" << std::endl; return 1; }
    std::cout << "Epoll 관제실 생성 성공 (Epoll FD: " << epoll_fd << ")" << std::endl;

    // 3. epoll_ctl (server_fd 등록)
    struct epoll_event event_config;         
    event_config.events = EPOLLIN;           
    event_config.data.fd = server_fd;        
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event_config) < 0) {
        std::cerr << "epoll에 server_fd 등록 실패!" << std::endl; return 1;
    }
    std::cout << "Epoll 관제실에 'server_fd' 등록 성공!" << std::endl;

    // 4. epoll_wait (메인 루프)
    struct epoll_event events_list[MAX_EVENTS];
    char buffer[BUFFER_SIZE]; 

    std::cout << "====== 서버 실행 중! (Ctrl+C로 종료) ======" << std::endl;

    while (true)
    {
        int event_count = epoll_wait(epoll_fd, events_list, MAX_EVENTS, -1);
        if (event_count < 0) { std::cerr << "epoll_wait 실패!" << std::endl; break; }

        for (int i = 0; i < event_count; ++i)
        {
            int current_fd = events_list[i].data.fd;
            
            if (current_fd == server_fd) // 1. 새 연결
            {
                int client_fd = accept(server_fd, NULL, NULL);
                if (client_fd < 0) { std::cerr << "accept 실패!" << std::endl; continue; }
                std::cout << "새 클라이언트 연결 성공! (Client FD: " << client_fd << ")" << std::endl;
                
                event_config.events = EPOLLIN; 
                event_config.data.fd = client_fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event_config);
                client_buffers[client_fd] = std::vector<char>(); 
            }
            else // 2. 기존 클라이언트 이벤트
            {
                int read_len = read(current_fd, buffer, BUFFER_SIZE);

                if (read_len > 0)
                {
                    // '읽은 데이터'를 '개인 가방'에 넣기
                    std::vector<char>& recv_buffer = client_buffers[current_fd];
                    recv_buffer.insert(recv_buffer.end(), buffer, buffer + read_len);
                    
                    std::cout << "클라이언트(FD: " << current_fd << ")로부터 " << read_len << " 바이트 수신." 
                              << " (총 보관량: " << recv_buffer.size() << " 바이트)" << std::endl;

                    // '가방' 검사하러 보내기
                    HandleBuffer(current_fd, recv_buffer); 
                }
                else // read_len <= 0 (연결 끊김 또는 오류)
                {
                    if (read_len == 0)
                        std::cout << "클라이언트(FD: " << current_fd << ") 연결 끊어짐." << std::endl;
                    else
                        std::cerr << "클라이언트(FD: " << current_fd << ") read 오류!" << std::endl;
                    
                    CleanupPlayer(current_fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, NULL);
                    close(current_fd);
                    client_buffers.erase(current_fd); 
                }
            }
        } // end for
    } // end while

    if (ws_server) {
        ws_server->stop();
    }
    close(server_fd); 
    close(epoll_fd);  
    return 0; 
}