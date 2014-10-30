#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

int main(int argc,char *argv[])
{
    char buf[256];
    char str[10];
    int ttl;
    if (argc != 3)
    {
        printf("Wrong argument number for %s\n.", argv[0]);
        printf("Usage:\n %s host maxttl", argv[0]);
        return 1;
    }

    strcpy(str, argv[2]);
    ttl = atoi(str);
	//compile as root, chmod 4755, rename executable as tr_as_root.out
    setuid( 0 );
    snprintf(buf, sizeof buf, "traceroute -I -n -m %d %s", ttl, argv[1]);
    system( buf );

    return 0;
}
