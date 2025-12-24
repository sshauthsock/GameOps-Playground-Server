#include "websocket_server.h"
#include <iostream>
#include <algorithm>

// WebSocketSession 구현
WebSocketSession::WebSocketSession(tcp::socket socket, int client_fd,
                                   std::function<void(int, const std::vector<char>&)> handler)
    : ws_(std::move(socket))
    , client_fd_(client_fd)
    , message_handler_(handler)
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
        return;
    }

    if (ec)
    {
        fail(ec, "read");
        return;
    }

    // 메시지를 벡터로 변환 (바이너리 데이터 지원)
    auto data = buffer_.data();
    std::vector<char> message_data(static_cast<const char*>(data.data()), 
                                   static_cast<const char*>(data.data()) + data.size());

    std::cout << "[WebSocket] 메시지 수신 (FD: " << client_fd_
              << ", 크기: " << bytes_transferred << " bytes)" << std::endl;

    // 기존 TCP 서버의 메시지 핸들러 호출
    if (message_handler_)
    {
        message_handler_(client_fd_, message_data);
    }

    buffer_.consume(buffer_.size());
    do_read();
}

void WebSocketSession::send_message(const std::vector<char>& data)
{
    ws_.async_write(
        net::buffer(data.data(), data.size()),
        beast::bind_front_handler(
            &WebSocketSession::on_write,
            shared_from_this()));
}

void WebSocketSession::on_write(beast::error_code ec, std::size_t bytes_transferred)
{
    if (ec)
    {
        fail(ec, "write");
        return;
    }
}

void WebSocketSession::fail(beast::error_code ec, char const* what)
{
    std::cerr << "[WebSocket] " << what << ": " << ec.message() << std::endl;
}

// WebSocketServer 구현
WebSocketServer::WebSocketServer(unsigned short port,
                                std::function<void(int, const std::vector<char>&)> handler)
    : acceptor_(ioc_, tcp::endpoint(tcp::v4(), port))
    , message_handler_(handler)
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
    acceptor_.async_accept(
        [this](beast::error_code ec, tcp::socket socket)
        {
            if (!ec)
            {
                int client_fd = next_client_fd_++;
                auto session = std::make_shared<WebSocketSession>(
                    std::move(socket), client_fd, message_handler_);
                sessions_[client_fd] = session;
                session->run();
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
    auto it = sessions_.find(client_fd);
    if (it != sessions_.end())
    {
        it->second->send_message(data);
    }
}

void WebSocketServer::close_client(int client_fd)
{
    sessions_.erase(client_fd);
}

