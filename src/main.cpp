#include "../include/RedisServer.h"
#include "../include/RedisDatabase.h"
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

int main(int argc, char* argv[]) {
    int port = 6379; //default
    if(argc >= 2) port = std::stoi(argv[1]);

    if (RedisDatabase::getInstance().load("dump.my_rdb"))
        std::cout << "Database loaded from dump.my_rdb\n";
    else
        std::cout << "No existing database found or load failed. Starting fresh.\n";

    RedisServer server(port);

    //background persistance: dump the database every 300 seconds. (5 * 60 seconds save database)
    std::thread persistanceThread([](){
        while (false) {
            std::this_thread::sleep_for(std::chrono::seconds(300));
            //dump the database
            if(!RedisDatabase::getInstance().dump("dump.my_rdb"))
                std::cerr << "Error Dumping Database\n";
            else    
                std::cout << "Database Dumped to dump.my_rdb\n";
        }
    });
    persistanceThread.detach();

    server.run();
    return 0;
}