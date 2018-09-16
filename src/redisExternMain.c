#include "server.h"
#include "zmalloc.h"
#include "loadlib.h"

reWorkerInfo * findBestThread(struct reWorkerTable *workerTable)
{
	int size = workerTable->currWorkers;
	int worker;
	reWorkerInfo *bestWorker, *tmpWorker;
	if(size > 0) {
		bestWorker = &workerTable->workers[0];
		for(worker = 1; worker < size; worker++) {
			tmpWorker = &workerTable->workers[worker];
			if(tmpWorker->workerList->len < bestWorker->workerList->len) {
				bestWorker = tmpWorker;
			}
		}

		return bestWorker;
	} else {
		return NULL;
	} 
}

struct reCombinerInfo * combinerInit()
{
	struct reCombinerInfo *combiner;
	combiner = (reCombinerInfo *)zmalloc(sizeof(reCombinerInfo));
	combiner->combinerRun = 1;
	combiner->channel = NULL;
	combiner->combinerList = listCreate();
    if(pthread_create(&(combiner->combinerId),NULL,combinerThread,(void *)combiner) != 0) {
		fprintf(stderr,"can't create pthread\n");
    }
	return combiner;
}

struct reWorkerTable * workerInit(int workerSize)
{
	reWorkerTable *workerTable;
	int i;
	workerTable = (reWorkerTable*)zmalloc(sizeof(reWorkerTable));
	workerTable->currWorkers = workerSize;
	workerTable->workers = (struct reWorkerInfo *) zmalloc(sizeof(reWorkerInfo)*workerSize);
	reWorkerInfo * workerinfo = workerTable->workers;
	for(i = 0; i < workerSize; i++) {
		workerinfo[i].workerList = listCreate();
		workerinfo[i].workerRun = 1;
		 workerinfo[i].channel = NULL;
		if(pthread_create(&(workerinfo[i].workerId),NULL,workerThread,&(workerinfo[i])) != 0) {
			fprintf(stderr,"can't create pthread\n");
		}
	}
}

int sendToWork(reWorkerInfo *worker, client *clientData)
{
	if(worker->channel == NULL) {
		worker->channel = (char *)zmalloc(sizeof(char)*CHANNEL_LEN);
		sprintf(worker->channel, "%s_%ld", WORKER, worker->workerId);
	}
	char *workerChannel = worker->channel;
	int res;
	if(!channelIsCreate(workerChannel)) {
		res = createChannel(workerChannel);	
	}

	int writeMode = WR_BLOCK;
	int pipefd = openChannel(workerChannel, writeMode);
	return writeChannel(pipefd, clientData);
}

int processCommandRE(client *clientData)
{
	reWorkerInfo * worker = findBestThread(workerTable);
	sendToWork(worker, clientData);
}

void  parseQuery(client * clientData)
{

/*
 * do parese
 *
 */	

//	sendToCombiner();

}

int sendToCombiner(reCombinerInfo *combiner)
{
}
void *workerThread(void *value)
{


/*
 * recFromPrimary()
 *
 * parseQuery()
 *
 * processCommandHook()-->sendToCombiner()
 *
 *
 *
 */
    int res = 0;
	reWorkerInfo *worker = (reWorkerInfo *)value;
    list *l = worker->workerList;
	if(worker->channel == NULL) {
		worker->channel = (char *)zmalloc(sizeof(char)*CHANNEL_LEN);
		sprintf(worker->channel,"%s_%ld", WORKER, worker->workerId);
	}
	char *workerChannel = worker->channel;
	//char *combinerChannel = (char *)zmalloc(sizeof(char)*CHANNEL_LEN);
	//sprintf(combinerChannel,"%s_%ld", COMINBER, combinerId);
	int run = worker->workerRun;
    if(!channelIsCreate(workerChannel)){
        res = createChannel(workerChannel);
    }
/*
    if(!channelIsCreate(combinerChannel)){
        res = createChannel(combinerChannel);
    }
    
*/    
    int readMode = RD_NOBLOCK;
//    int writeMode = WR_BLOCK;
    int fdPrimaryToWorker = openChannel(workerChannel, readMode);
//    int fdWorkerToCombiner = openChannel(combinerChannel, writeMode);
    int readlen = 0;
    while(run) {
        while(l->len == 0 || readlen > 0) {
            readlen = readChannel(fdPrimaryToWorker, l);
            sleep(1);
        }
        while(l->len > 0) {
            listNode *node = listPop(l);
			client * clientData = (client *)turnToAddr(node->value);

			parseQuery(clientData);
            //char * d = (char*)zmalloc(sizeof(void*)*10);
            //sprintf(d,"worker!");
            //writeChannel(fdWorkerToCombiner,(char *)turnToAddr(node->value));
            zfree(node);
        }
    }
    
//    close(fdWorkerToCombiner);
    close(fdPrimaryToWorker);
    
}

void beforeSleepRE(struct aeEventLoop *eventLoop)
{
/*
 * we do nothing here, combiner thread will do it.
 */
}


void *combinerThread(void *value)
{
	struct reCombinerInfo *combiner = (reCombinerInfo *)value;
    list *l = combiner->combinerList;

	if(combiner->channel == NULL) {
		combiner->channel = (char *)zmalloc(sizeof(char)*CHANNEL_LEN);
		sprintf(combiner->channel,"%s_%ld", COMINBER, combiner->combinerId);
	}
	char *channel = combiner->channel;
    int fd;
    if(!channelIsCreate(channel)){
        fd = createChannel(channel);
    }
	int run = combiner->combinerRun;
    int readMode = RD_NOBLOCK;
    int pipefd = openChannel(channel, readMode);
    int readlen = 0;
    while(run) {
        while(l->len == 0 || readlen > 0) {
            readlen = readChannel(pipefd, l);
            sleep(1);
        }
        while(l->len > 0) {
            listNode *node = listPop(l);
//			execCommandRE();
            printf("in comber:%s\n",(char *)turnToAddr(node->value));
            zfree(node);
        }
//
//standBeforeSleep();
    }
    return NULL;
}

/*
int main()
{

    pthread_t pCombiner, pWorker, pPrimary;

    int primaryId = pthread_create(&pPrimary, NULL, primary, NULL);
    int workerId = pthread_create(&pWorker, NULL, worker, NULL);
    int combinerId = pthread_create(&pCombiner, NULL, combiner, NULL);

    pthread_join(pPrimary, NULL);
    pthread_join(pWorker, NULL);
    pthread_join(pCombiner, NULL);

    return 0;
}

*/
