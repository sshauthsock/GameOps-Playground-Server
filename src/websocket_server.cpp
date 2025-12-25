#include "websocket_server.h"
#include <iostream>
#include <algorithm>

// WebSocketSession 구현
WebSocketSession::WebSocketSession(tcp::socket socket, int client_fd,
                                   std::function<void(int, const std::vector<char>&)> handler,
                                   std::function<void(int)> close_callback)
    : ws_(std::move(socket))
    , client_fd_(client_fd)
    , message_handler_(handler)
    , close_session_callback_(close_callback)
    , is_writing_(false)
{
}

void WebSocketSession::run()
{
    ws_.async_accept(
        beast::bind_front_handler(
            &WebSocketSession::on_accept,
            shared_from_this()));
}

void WebSocketSession::on_accept(beast::error_code ec)
{
    if (ec)
    {
        std::cerr << "[WebSocket] accept 실패 (FD: " << client_fd_ 
                  << "): " << ec.message() << std::endl;
        fail(ec, "accept");
        return;
    }

    // 바이너리 모드 설정
    ws_.binary(true);
    
    // 메시지 크기 제한 제거 (기본값은 64KB, 게임 패킷을 위해 제한 없음)
    ws_.read_message_max(0);  // 0 = 제한 없음
    
    std::cout << "[WebSocket] 클라이언트 연결 성공 (FD: " << client_fd_ << ")" << std::endl;
    do_read();
}

void WebSocketSession::do_read()
{
    ws_.async_read(
        buffer_,
        beast::bind_front_handler(
            &WebSocketSession::on_read,
            shared_from_this()));
}

void WebSocketSession::on_read(beast::error_code ec, std::size_t bytes_transferred)
{
    if (ec == websocket::error::closed)
    {
        std::cout << "[WebSocket] 클라이언트 연결 종료 (FD: " << client_fd_ << ")" << std::endl;
        // 세션 제거 콜백 호출
        if (close_session_callback_)
        {
            close_session_callback_(client_fd_);
        }
        return;
    }

    if (ec)
    {
        fail(ec, "read");
        return;
    }

    // WebSocket 메시지가 완전히 수신되었는지 확인
    // bytes_transferred는 이번 읽기에서 받은 바이트 수
    // buffer_.size()는 현재 버퍼에 있는 전체 바이트 수
    std::size_t buffer_size = buffer_.size();
    
    // 비정상적으로 큰 메시지 검증 (100MB 이상)
    if (buffer_size > 100 * 1024 * 1024)
    {
        std::cerr << "[WebSocket] 비정상적으로 큰 메시지 수신: " << buffer_size 
                  << " bytes. 연결 종료." << std::endl;
        buffer_.consume(buffer_.size());
        return;
    }

    // 메시지를 벡터로 변환 (바이너리 데이터 지원)
    auto data = buffer_.data();
    std::vector<char> message_data(static_cast<const char*>(data.data()), 
                                   static_cast<const char*>(data.data()) + buffer_size);

    std::cout << "[WebSocket] 메시지 수신 (FD: " << client_fd_
              << ", 크기: " << buffer_size << " bytes)" << std::endl;

    // 기존 TCP 서버의 메시지 핸들러 호출
    if (message_handler_)
    {
        message_handler_(client_fd_, message_data);
    }

    // 버퍼 정리 (모든 데이터 소비)
    buffer_.consume(buffer_.size());
    do_read();
}

void WebSocketSession::send_message(const std::vector<char>& data)
{
    // 큐에 메시지 추가
    write_queue_.push_back(data);
    
    std::cout << "[WebSocket] 메시지 큐에 추가 (FD: " << client_fd_ 
              << ", 크기: " << data.size() << " bytes, 큐 크기: " 
              << write_queue_.size() << ")" << std::endl;
    
    // 현재 write 작업이 진행 중이 아니면 시작
    if (!is_writing_)
    {
        do_write();
    }
}

void WebSocketSession::do_write()
{
    if (write_queue_.empty())
    {
        is_writing_ = false;
        return;
    }
    
    is_writing_ = true;
    std::vector<char> message = write_queue_.front();
    write_queue_.erase(write_queue_.begin());
    
    std::cout << "[WebSocket] 메시지 전송 시작 (FD: " << client_fd_ 
              << ", 크기: " << message.size() << " bytes, 큐에 남은 메시지: " 
              << write_queue_.size() << ")" << std::endl;
    
    ws_.async_write(
        net::buffer(message.data(), message.size()),
        beast::bind_front_handler(
            &WebSocketSession::on_write,
            shared_from_this()));
}

void WebSocketSession::on_write(beast::error_code ec, std::size_t bytes_transferred)
{
    if (ec)
    {
        fail(ec, "write");
        is_writing_ = false;
        return;
    }
    
    std::cout << "[WebSocket] 메시지 전송 완료 (FD: " << client_fd_ 
              << ", 크기: " << bytes_transferred << " bytes)" << std::endl;
    
    // 다음 메시지 전송
    do_write();
}

void WebSocketSession::fail(beast::error_code ec, char const* what)
{
    std::cerr << "[WebSocket] " << what << ": " << ec.message() << " (FD: " << client_fd_ << ")" << std::endl;
    // 연결 오류 시 세션 제거 콜백 호출
    if (close_session_callback_)
    {
        close_session_callback_(client_fd_);
    }
}

// WebSocketServer 구현
WebSocketServer::WebSocketServer(unsigned short port,
                                std::function<void(int, const std::vector<char>&)> handler,
                                std::function<void(int)> cleanup_handler)
    : strand_(net::make_strand(ioc_.get_executor()))
    , acceptor_(ioc_, tcp::endpoint(tcp::v4(), port))
    , message_handler_(handler)
    , cleanup_callback_(cleanup_handler)
    , next_client_fd_(10000)  // TCP FD와 겹치지 않도록 큰 수로 시작
{
}

WebSocketServer::~WebSocketServer()
{
    stop();
}

void WebSocketServer::start()
{
    do_accept();
    server_thread_ = std::thread(&WebSocketServer::run, this);
    std::cout << "[WebSocket] 서버 시작 (포트: " << acceptor_.local_endpoint().port() << ")" << std::endl;
}

void WebSocketServer::stop()
{
    ioc_.stop();
    if (server_thread_.joinable())
    {
        server_thread_.join();
    }
}

void WebSocketServer::do_accept()
{
    std::cout << "[WebSocketServer] do_accept 호출 - 새 연결 대기 중..." << std::endl;
    acceptor_.async_accept(
        [this](beast::error_code ec, tcp::socket socket)
        {
            std::cout << "[WebSocketServer] async_accept 콜백 실행 (오류: " << (ec ? ec.message() : "없음") << ")" << std::endl;
            if (!ec)
            {
                std::cout << "[WebSocketServer] TCP 연결 수락 성공, strand로 전달 중..." << std::endl;
                // strand를 통해 FD 할당 및 세션 저장 (스레드 안전성 보장)
                net::post(strand_, [this, socket = std::move(socket)]() mutable {
                    int client_fd = next_client_fd_++;
                    std::cout << "[WebSocketServer] 새 클라이언트 연결 수락 (FD: " << client_fd << ")" << std::endl;
                    
                    auto session = std::make_shared<WebSocketSession>(
                        std::move(socket), client_fd, message_handler_,
                        [this](int fd) { 
                            std::cout << "[WebSocketServer] 세션 제거 콜백 호출 (FD: " << fd << ")" << std::endl;
                            // 플레이어 정리 콜백 호출 (연결 종료 시 플레이어 정리 및 빈 방 삭제)
                            // cleanup_callback_는 CleanupPlayer를 호출하는데,
                            // 이미 세션이 제거되는 중이므로 close_websocket_session=false로 호출해야 함
                            // 하지만 cleanup_callback_는 int만 받으므로, 
                            // CleanupPlayer 내부에서 이미 세션이 제거되었는지 확인하도록 수정
                            if (cleanup_callback_)
                            {
                                cleanup_callback_(fd);
                            }
                            // 세션 제거 (cleanup 콜백 호출 후)
                            this->close_client(fd); 
                        });
                    
                    sessions_[client_fd] = session;
                    std::cout << "[WebSocketServer] 세션 저장 완료 (FD: " << client_fd 
                              << ", 총 세션 수: " << sessions_.size() << ")" << std::endl;
                    
                    session->run();
                });
            }
            else
            {
                std::cerr << "[WebSocketServer] 연결 수락 오류: " << ec.message() << std::endl;
            }
            do_accept();
        });
}

void WebSocketServer::run()
{
    ioc_.run();
}

void WebSocketServer::send_to_client(int client_fd, const std::vector<char>& data)
{
    // strand를 사용하여 스레드 안전성 보장 및 순차 실행
    net::post(strand_, [this, client_fd, data]() {
        do_send_to_client(client_fd, data);
    });
}

void WebSocketServer::do_send_to_client(int client_fd, const std::vector<char>& data)
{
    std::cout << "[WebSocketServer] do_send_to_client 실행 (FD: " << client_fd 
              << ", 메시지 크기: " << data.size() << " bytes, 총 세션 수: " 
              << sessions_.size() << ")" << std::endl;
    
    auto it = sessions_.find(client_fd);
    if (it != sessions_.end())
    {
        std::cout << "[WebSocketServer] 클라이언트 찾음 (FD: " << client_fd 
                  << "), 메시지 전송 시작" << std::endl;
        it->second->send_message(data);
    }
    else
    {
        std::cerr << "[WebSocketServer] 오류: 클라이언트를 찾을 수 없음 (FD: " 
                  << client_fd << ")" << std::endl;
        std::cerr << "[WebSocketServer] 현재 세션 목록: ";
        for (const auto& pair : sessions_)
        {
            std::cerr << pair.first << " ";
        }
        std::cerr << std::endl;
        
        // 세션이 없는 경우 디버깅 정보 출력
        std::cerr << "[WebSocketServer] 디버깅: 세션이 생성되었는지 확인 필요" << std::endl;
    }
}

void WebSocketServer::close_client(int client_fd)
{
    // strand를 사용하여 스레드 안전성 보장 및 순차 실행
    net::post(strand_, [this, client_fd]() {
        auto it = sessions_.find(client_fd);
        if (it != sessions_.end())
        {
            std::cout << "[WebSocketServer] 세션 제거 (FD: " << client_fd 
                      << ", 제거 전 세션 수: " << sessions_.size() << ")" << std::endl;
            sessions_.erase(it);
            std::cout << "[WebSocketServer] 세션 제거 완료 (제거 후 세션 수: " 
                      << sessions_.size() << ")" << std::endl;
        }
        else
        {
            std::cerr << "[WebSocketServer] 경고: 제거하려는 세션을 찾을 수 없음 (FD: " 
                      << client_fd << ")" << std::endl;
        }
    });
}

