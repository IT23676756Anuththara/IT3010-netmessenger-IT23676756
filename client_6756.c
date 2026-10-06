#include <stdio.h>

#define REG_NO "IT23676756"
#define PORT   12756

int main(void) {
    printf("NetMessenger client (%s): will connect to port %d\n", REG_NO, PORT);
    return 0;
}
