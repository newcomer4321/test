#include "server.h"
#include "loadlib.h"
//fprintf

int channelIsCreate(char *channelName)
{
    int res = 1;
    if(access(channelName, F_OK) == -1) {
        res = 0;
    } 

    return res;

}


int createChannel(char *channelName)
{
    int res = 0;
    res = mkfifo(channelName, 0777);
    if(res != 0) {
        fprintf(stderr, "can't create fifo %s\n",channelName);
    }

    return res;
}

int openChannel(char *channelName, int mode)
{
    int pipefd = open(channelName, mode);
    if(pipefd < 0) {
        fprintf(stderr, "can't open pipe %s\n",channelName);
    } 
    return pipefd;
}

list *listPush(list *l, void *value)
{
    return listAddNodeTail(l, value);
}


listNode *listPop(list *l)
{

    listNode * node = listFirst(l);

    if (node->prev)
        node->prev->next = node->next;
    else
        l->head = node->next;
    if (node->next)
        node->next->prev = node->prev;
    else
        l->tail = node->prev;

    l->len--;
    return node;    
}


int readChannel(int pipefd, list *l)
{
    
    int readlen;
    void *buf = zmalloc(CLIENT_POINT_SIZE);

    if(l == NULL) {
        l = listCreate();
    }


    do{
        readlen = read(pipefd, buf, CLIENT_POINT_SIZE);
        if(readlen > 0) {
//            l = listPush(l,(void*)(*((int*)buf)));
            l = listPush(l,buf);
            buf = NULL;
            buf = zmalloc(CLIENT_POINT_SIZE);
        }
    }while(readlen > 0);
    
    zfree(buf);
    
    return readlen;

}

int writeChannel(int pipefd, void* value)
{
    int writelen;
   // int32_t addr = value;
    
    writelen = write(pipefd, &value, CLIENT_POINT_SIZE);

    if(writelen != CLIENT_POINT_SIZE){
        fprintf(stderr, "can't write pipe %d sizeof %ld\n",pipefd, CLIENT_POINT_SIZE);
    }
    
    return writelen;
}

