#include <stdio.h>

#define REG_NO "IT23676756"
#define PORT   12756
#define NID    "NID:6767"

int main(void) {
    printf("NetMessenger server (%s): port %d, tag %s\n", REG_NO, PORT, NID);
    return 0;
}
