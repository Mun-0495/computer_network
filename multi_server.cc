#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>
#include <time.h>

#define BUFSIZE 1024
#define MAX_CLIENT  64
#define NICK_LEN 32

typedef struct {
    int sock;
    char nick[NICK_LEN];
    time_t connected_at;
} client_info_t;

typedef struct {
    int sock;
    struct sockaddr_in addr;
} thread_arg_t;

client_info_t clients[MAX_CLIENT];
int clnt_count = 0;
pthread_mutex_t clnt_mutex = PTHREAD_MUTEX_INITIALIZER;

void error_handling(const char *message)
{
        fputs(message, stderr);
        fputc('\n', stderr);
        exit(-1);
}

void trim_newline(char* s)
{
    size_t len = strlen(s);
    while(len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r')){
        s[len - 1] = '\0';
        len--;
    }
}

void make_time_prefix(char* out, size_t out_size)
{
    time_t now = time(NULL);
    struct tm tm_now;

    localtime_r(&now, &tm_now);
    strftime(out, out_size, "[%H:%M:%S]", &tm_now);
}

void send_to_sock(int sock, const char* msg)
{
    if(sock < 0 || msg == NULL){
        return;
    }
    write(sock, msg, strlen(msg));
}

void broadcast_message(const char* msg, int exclude_sock);

int is_nick_taken(const char* nick)
{
    int i;
    for(i = 0; i < clnt_count; i++){
        if(strcmp(clients[i].nick, nick) == 0){
            return 1;
        }
    }
    return 0;
}

void send_system_message(int sock, const char* message)
{
    char timebuf[16];
    char line[BUFSIZE + 32];

    make_time_prefix(timebuf, sizeof(timebuf));
    snprintf(line, sizeof(line), "%s [SYSTEM] %s\n", timebuf, message);
    send_to_sock(sock, line);
}

void broadcast_system_message(const char* message, int exclude_sock)
{
    char timebuf[16];
    char line[BUFSIZE + 32];

    make_time_prefix(timebuf, sizeof(timebuf));
    snprintf(line, sizeof(line), "%s [SYSTEM] %s\n", timebuf, message);
    broadcast_message(line, exclude_sock);
}

void broadcast_chat_message(const char* nick, const char* message, int exclude_sock)
{
    char timebuf[16];
    char line[BUFSIZE + NICK_LEN + 32];

    make_time_prefix(timebuf, sizeof(timebuf));
    snprintf(line, sizeof(line), "%s [%s] %s\n", timebuf, nick, message);
    broadcast_message(line, exclude_sock);
}

void broadcast_message(const char* msg, int exclude_sock)
{
    int i;
    pthread_mutex_lock(&clnt_mutex);
    for(i = 0; i < clnt_count; i++){
        if(clients[i].sock != exclude_sock){
            send_to_sock(clients[i].sock, msg);
        }
    }
    pthread_mutex_unlock(&clnt_mutex);
}

int add_client(int sock, const char* nick)
{
    int idx = -1;

    pthread_mutex_lock(&clnt_mutex);
    if(clnt_count < MAX_CLIENT){
        idx = clnt_count;
        clients[idx].sock = sock;
        strncpy(clients[idx].nick, nick, NICK_LEN - 1);
        clients[idx].nick[NICK_LEN - 1] = '\0';
        clients[idx].connected_at = time(NULL);
        clnt_count++;
    }
    pthread_mutex_unlock(&clnt_mutex);

    return idx;
}

int remove_client(int sock, char* out_nick)
{
    int i;
    int found = 0;

    pthread_mutex_lock(&clnt_mutex);
    for(i = 0; i < clnt_count; i++){
        if(clients[i].sock == sock){
            if(out_nick != NULL){
                strncpy(out_nick, clients[i].nick, NICK_LEN - 1);
                out_nick[NICK_LEN - 1] = '\0';
            }
            while(i < clnt_count - 1){
                clients[i] = clients[i + 1];
                i++;
            }
            clnt_count--;
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&clnt_mutex);

    return found;
}

void get_nick_by_sock(int sock, char* out_nick)
{
    int i;
    if(out_nick == NULL){
        return;
    }

    out_nick[0] = '\0';

    pthread_mutex_lock(&clnt_mutex);
    for(i = 0; i < clnt_count; i++){
        if(clients[i].sock == sock){
            strncpy(out_nick, clients[i].nick, NICK_LEN - 1);
            out_nick[NICK_LEN - 1] = '\0';
            break;
        }
    }
    pthread_mutex_unlock(&clnt_mutex);
}

int change_nick(int sock, const char* new_nick, char* old_nick)
{
    int i;
    int updated = 0;

    pthread_mutex_lock(&clnt_mutex);
    if(is_nick_taken(new_nick)){
        pthread_mutex_unlock(&clnt_mutex);
        return 0;
    }

    for(i = 0; i < clnt_count; i++){
        if(clients[i].sock == sock){
            if(old_nick != NULL){
                strncpy(old_nick, clients[i].nick, NICK_LEN - 1);
                old_nick[NICK_LEN - 1] = '\0';
            }
            strncpy(clients[i].nick, new_nick, NICK_LEN - 1);
            clients[i].nick[NICK_LEN - 1] = '\0';
            updated = 1;
            break;
        }
    }
    pthread_mutex_unlock(&clnt_mutex);

    return updated;
}

int find_sock_by_nick(const char* nick)
{
    int i;
    int target_sock = -1;

    pthread_mutex_lock(&clnt_mutex);
    for(i = 0; i < clnt_count; i++){
        if(strcmp(clients[i].nick, nick) == 0){
            target_sock = clients[i].sock;
            break;
        }
    }
    pthread_mutex_unlock(&clnt_mutex);

    return target_sock;
}

void send_user_list(int requester_sock)
{
    int i;
    char line[BUFSIZE];
    time_t now = time(NULL);

    send_system_message(requester_sock, "===== 접속자 목록 =====");

    pthread_mutex_lock(&clnt_mutex);
    for(i = 0; i < clnt_count; i++){
        long sec = (long)difftime(now, clients[i].connected_at);
        snprintf(line, sizeof(line), "%s (접속 %ld초)", clients[i].nick, sec);
        send_system_message(requester_sock, line);
    }
    pthread_mutex_unlock(&clnt_mutex);

    send_system_message(requester_sock, "=====================");
}

void send_help_message(int requester_sock)
{
    send_system_message(requester_sock, "도움말 / 기능 안내");
    send_system_message(requester_sock, "/nick 이름 : 닉네임 변경");
    send_system_message(requester_sock, "/w 닉네임 내용 : 귓속말");
    send_system_message(requester_sock, "/who : 접속자 목록 확인");
    send_system_message(requester_sock, "/help : 명령어 목록 다시 보기");
    send_system_message(requester_sock, "/quit : 종료");
}

int process_line_message(int clnt_sock, char* msg, char* my_nick)
{
    if(strlen(msg) == 0){
        return 1;
    }

    if(strncmp(msg, "/nick ", 6) == 0){
        char* new_nick = msg + 6;
        char old_nick[NICK_LEN];
        char notice[BUFSIZE];

        while(*new_nick == ' '){
            new_nick++;
        }

        if(strlen(new_nick) == 0 || strlen(new_nick) >= NICK_LEN){
            send_system_message(clnt_sock, "올바른 닉네임 길이(1~31자)를 입력하세요.");
            return 1;
        }

        if(change_nick(clnt_sock, new_nick, old_nick)){
            strncpy(my_nick, new_nick, NICK_LEN - 1);
            my_nick[NICK_LEN - 1] = '\0';
            snprintf(notice, sizeof(notice), "%s 님의 닉네임이 %s(으)로 변경되었습니다.", old_nick, my_nick);
            broadcast_system_message(notice, -1);
        } else {
            send_system_message(clnt_sock, "이미 사용 중인 닉네임입니다.");
        }
        return 1;
    }

    if(strncmp(msg, "/w ", 3) == 0){
        char* cursor = msg + 3;
        char* target_nick;
        char* body;
        int target_sock;
        char pm_msg[BUFSIZE];

        while(*cursor == ' '){
            cursor++;
        }
        target_nick = cursor;
        while(*cursor != '\0' && *cursor != ' '){
            cursor++;
        }
        if(*cursor == '\0'){
            send_system_message(clnt_sock, "사용법: /w 대상닉네임 내용");
            return 1;
        }
        *cursor = '\0';
        cursor++;
        while(*cursor == ' '){
            cursor++;
        }
        body = cursor;

        if(strlen(body) == 0){
            send_system_message(clnt_sock, "귓속말 내용을 입력하세요.");
            return 1;
        }

        target_sock = find_sock_by_nick(target_nick);
        if(target_sock < 0){
            send_system_message(clnt_sock, "해당 닉네임 사용자를 찾을 수 없습니다.");
            return 1;
        }

        snprintf(pm_msg, sizeof(pm_msg), "[귓속말][%s -> %s] %s", my_nick, target_nick, body);
        send_system_message(target_sock, pm_msg);
        if(target_sock != clnt_sock){
            send_system_message(clnt_sock, pm_msg);
        }
        return 1;
    }

    if(strcmp(msg, "/who") == 0){
        send_user_list(clnt_sock);
        return 1;
    }

    if(strcmp(msg, "/help") == 0){
        send_help_message(clnt_sock);
        return 1;
    }

    if(strcmp(msg, "/quit") == 0){
        return 0;
    }

    broadcast_chat_message(my_nick, msg, clnt_sock);

    return 1;
}

void* handle_clnt(void* arg)
{
    thread_arg_t* t_arg = (thread_arg_t*)arg;
    int clnt_sock = t_arg->sock;
    struct sockaddr_in clnt_addr = t_arg->addr;
    free(arg);

    int str_len;
    char raw_msg[BUFSIZE];
    char pending[BUFSIZE * 4];
    int pending_len = 0;
    char my_nick[NICK_LEN];
    char join_msg[BUFSIZE];
    char ip_str[INET_ADDRSTRLEN];

    inet_ntop(AF_INET, &clnt_addr.sin_addr, ip_str, sizeof(ip_str));
    snprintf(my_nick, sizeof(my_nick), "Guest%d", clnt_sock);

    if(add_client(clnt_sock, my_nick) < 0){
        send_to_sock(clnt_sock, "[SYSTEM] 서버 수용 인원을 초과했습니다.\n");
        close(clnt_sock);
        return NULL;
    }

    send_system_message(clnt_sock, "채팅 서버에 연결되었습니다.");
    send_system_message(clnt_sock, "명령어: /nick 이름, /w 대상 내용, /who, /help, /quit");

    snprintf(join_msg, sizeof(join_msg), "%s 님이 입장했습니다. (%s:%d)",
             my_nick, ip_str, ntohs(clnt_addr.sin_port));
    broadcast_system_message(join_msg, -1);

    /*
      TCP는 메시지 경계가 보장되지 않으므로, 수신 버퍼를 누적한 뒤 '\n' 기준으로
      한 줄씩 분리해서 명령을 처리한다.
    */
    while((str_len = read(clnt_sock, raw_msg, sizeof(raw_msg) - 1)) > 0){
        int i;
        int start = 0;

        raw_msg[str_len] = '\0';

        if(pending_len + str_len >= (int)sizeof(pending)){
            pending_len = 0;
            send_system_message(clnt_sock, "메시지가 너무 길어 버퍼를 초기화했습니다.");
        }

        memcpy(pending + pending_len, raw_msg, str_len);
        pending_len += str_len;

        for(i = 0; i < pending_len; i++){
            if(pending[i] == '\n'){
                char line[BUFSIZE];
                int line_len = i - start + 1;

                if(line_len >= (int)sizeof(line)){
                    line_len = (int)sizeof(line) - 1;
                }

                memcpy(line, pending + start, line_len);
                line[line_len] = '\0';
                trim_newline(line);

                if(process_line_message(clnt_sock, line, my_nick) == 0){
                    str_len = -1;
                    break;
                }
                start = i + 1;
            }
        }

        if(str_len < 0){
            break;
        }

        if(start > 0){
            memmove(pending, pending + start, pending_len - start);
            pending_len -= start;
        }
    }

    {
        char leave_nick[NICK_LEN] = "";
        char leave_msg[BUFSIZE];
        if(remove_client(clnt_sock, leave_nick)){
            snprintf(leave_msg, sizeof(leave_msg), "%s 님이 퇴장했습니다.", leave_nick);
            broadcast_system_message(leave_msg, -1);
        }
    }

    close(clnt_sock);
    return NULL;
}

int main(int argc, char* argv[])
{
    int serv_sock;
    int clnt_sock;
    pthread_t t_id;

    struct sockaddr_in serv_addr;
    struct sockaddr_in clnt_addr;
    unsigned int clnt_addr_size;

    if(argc != 2) {
        printf("Usage : %s <port>\n", argv[0]);
        exit(1);
    }

    serv_sock = socket(PF_INET, SOCK_STREAM, 0);    /* 서버 소켓 생성 */

    if(-1 == serv_sock){
        error_handling("socket() error");
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(atoi(argv[1]));

    /* 소켓에 주소 할당 */
    if(-1 == bind(serv_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr) )){
        error_handling("bind() error");
    }

    if(-1 == listen(serv_sock, 5)){  /* 연결 요청 대기 상태로 진입 */
            error_handling("listen() error");
    }

    while(1){
        thread_arg_t* t_arg;

        clnt_addr_size = sizeof(clnt_addr);
        /* 연결 요청 수락 */
        clnt_sock = accept(serv_sock, (struct sockaddr*)&clnt_addr, &clnt_addr_size);
        if(-1 == clnt_sock){
            continue;
        }

        t_arg = (thread_arg_t*)malloc(sizeof(thread_arg_t));
        if(NULL == t_arg){
            close(clnt_sock);
            continue;
        }

        t_arg->sock = clnt_sock;
        t_arg->addr = clnt_addr;

        pthread_create(&t_id, NULL, handle_clnt, t_arg);
        pthread_detach(t_id);
    }

    return 0;
}