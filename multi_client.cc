#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>
#include <signal.h>

#define BUFSIZE 1024

int g_sock = -1;
volatile sig_atomic_t g_running = 1;
pthread_mutex_t g_output_mutex = PTHREAD_MUTEX_INITIALIZER;

void error_handling(const char* message)
{
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(1);
}

void sigint_handler(int signo)
{
    (void)signo;
    g_running = 0;
    if(g_sock >= 0){
        shutdown(g_sock, SHUT_RDWR);
    }
}

void* recv_thread(void* arg)
{
    int sock = *(int*)arg;
    char msg[BUFSIZE];
    int len;

    while(g_running && (len = read(sock, msg, sizeof(msg) - 1)) > 0){
        msg[len] = '\0';
        pthread_mutex_lock(&g_output_mutex);
        fputs(msg, stdout);
        fflush(stdout);
        if(g_running){
            fputs("> ", stdout);
            fflush(stdout);
        }
        pthread_mutex_unlock(&g_output_mutex);
    }

    g_running = 0;
    return NULL;
}

void* send_thread(void* arg)
{
    int sock = *(int*)arg;
    char msg[BUFSIZE];

    while(g_running && fgets(msg, sizeof(msg), stdin) != NULL){
        if(write(sock, msg, strlen(msg)) < 0){
            break;
        }

        if(strncmp(msg, "/quit", 5) == 0){
            break;
        }

        pthread_mutex_lock(&g_output_mutex);
        fputs("> ", stdout);
        fflush(stdout);
        pthread_mutex_unlock(&g_output_mutex);
    }

    g_running = 0;
    shutdown(sock, SHUT_WR);
    return NULL;
}

int main(int argc, char* argv[])
{
    int sock;
    struct sockaddr_in serv_addr;
    pthread_t snd_t, rcv_t;

    /*
      사용법:
      ./multi_client <IP> <PORT> [NICKNAME]

      설계 포인트:
      1) 송신/수신을 각각 별도 스레드로 분리하여, 입력 대기 중에도 서버 메시지를 즉시 출력한다.
      2) 닉네임은 연결 직후 "/nick 이름" 명령으로 서버에 전달한다.
            3) /help 로 사용 가능한 명령어를 다시 확인할 수 있다.
            4) 종료는 /quit 또는 Ctrl+C 로 처리한다.
    */
    if(argc < 3 || argc > 4){
        printf("Usage: %s <IP> <PORT> [NICKNAME]\n", argv[0]);
        return 1;
    }

    signal(SIGINT, sigint_handler);

    sock = socket(PF_INET, SOCK_STREAM, 0);
    if(sock == -1){
        error_handling("socket() error");
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(atoi(argv[2]));

    if(inet_pton(AF_INET, argv[1], &serv_addr.sin_addr) <= 0){
        close(sock);
        error_handling("inet_pton() error");
    }

    if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1){
        close(sock);
        error_handling("connect() error");
    }

    g_sock = sock;

    if(argc == 4){
        char nick_cmd[BUFSIZE];
        snprintf(nick_cmd, sizeof(nick_cmd), "/nick %s\n", argv[3]);
        write(sock, nick_cmd, strlen(nick_cmd));
    }

    printf("Connected: %s:%s\n", argv[1], argv[2]);
    printf("Commands: /nick <name>, /w <name> <msg>, /me <action>, /search <keyword>, /who, /help, /quit\n");
    printf("> ");
    fflush(stdout);

    pthread_create(&rcv_t, NULL, recv_thread, (void*)&sock);
    pthread_create(&snd_t, NULL, send_thread, (void*)&sock);

    pthread_join(snd_t, NULL);
    pthread_join(rcv_t, NULL);

    close(sock);
    g_sock = -1;
    return 0;
}
