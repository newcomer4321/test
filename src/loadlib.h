#include <fcntl.h>  
#include <sys/stat.h> 
#include "adlist.h"
#include "zmalloc.h"
#include "server.h"
#include <pthread.h>
#define turnToAddr(addr)  (*((ssize_t*)addr))


#define BUFFER_SIZE PIPE_BUF

/*
#define loadDebug

#ifdef loadDebug


#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3


#define addReplyErrorFormat(c,tmp,args...) fprintf(stdout,tmp,##args)
#define serverLog(level, fmt, args...) printf(fmt, ##args)



#endif
*/
#define CLIENT_POINT_SIZE   (sizeof(void*))
#define RD_NOBLOCK  O_RDONLY | O_NONBLOCK
#define RD_BLOCK    O_RDONLY
#define WR_NOBLOCK  O_WRONLY | O_NONBLOCK
#define WR_BLOCK    O_WRONLY 

#define WORKER	"worker"
#define	COMINBER	"combiner"
#define CHANNEL_LEN	20
/*
 * re means redis extern,channel name will be worker+pthreadId.
 */
typedef struct reWorkerTable {
	int currWorkers;
	struct reWorkerInfo *workers;
}reWorkerTable;

typedef struct reWorkerInfo {
	pthread_t workerId;
	list * workerList;
	int workerRun;
	char *channel;
}reWorkerInfo;

typedef struct reCombinerInfo {
	pthread_t combinerId;
	int combinerRun;
	char *channel;
	list *combinerList;
}reCombinerInfo;
/*
typedef struct channelData {
	struct aeEventLoop *eventLoop;
	int fd;
	int flag;
	int mask;
	void *clientData;
}channelData;
*/
struct reCombinerInfo *combiner;
struct reWorkerTable *workerTable;

int createChannel(char *channelName);
int readChannel(int pipefd, list *l);
int writeChannel(int pipefd, void* value);
listNode *listPop(list *l);
list *listPush(list *l, void *value);
int channelIsCreate(char *channelName);
int openChannel(char *channelName, int mode);


reWorkerInfo * findBestThread(struct reWorkerTable *workerTable);
struct reCombinerInfo * combinerInit();
struct reWorkerTable * workerInit(int workerSize);
int sendToWork(reWorkerInfo *worker, client *clientData);

int processCommandRE(client *cilientData);
 void  parseQuery(client * clientData);
int sendToCombiner(reCombinerInfo *combiner);

void *workerThread(void *value);
void beforeSleepRE(struct aeEventLoop *eventLoop);


void *combinerThread(void *value);
