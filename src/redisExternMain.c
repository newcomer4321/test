#include "server.h"
#include "zmalloc.h"
#include "loadlib.h"
#include <string.h>
#include <stdlib.h>
extern aeBeforeSleepProc *aeBeforeSleepHook;
extern reProcessComandProc processComandProcREHook;
extern processInputBufferProc processInputBufferProcHook;
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
	return workerTable;
}

int sendToWork(reWorkerInfo *worker, client *clientData)
{
	if(worker->channel == NULL) {
		worker->channel = (char *)zmalloc(sizeof(char)*CHANNEL_LEN);
		sprintf(worker->channel, "%s_%ld", WORKER, worker->workerId);
	}
	char *workerChannel = worker->channel;
	if(!channelIsCreate(workerChannel)) {
		createChannel(workerChannel);	
	}

	int writeMode = WR_BLOCK;
	int pipefd = openChannel(workerChannel, writeMode);
	return writeChannel(pipefd, clientData);
}


void processInputBufferRE(client *c)
{
	reWorkerInfo * worker = findBestThread(workerTable);
	sendToWork(worker, c);
}


int processCommandRE(client *c)
{
/*
	reWorkerInfo * worker = findBestThread(workerTable);
	return sendToWork(worker, clientData);
}

int  parseQuery(client * c)
{
*/
/*
 * do parese
 *
 */	
	
	 if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        addReply(c,shared.ok);
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd) {
        flagTransaction(c);
        sds args = sdsempty();
        int i;
        for (i=1; i < c->argc && sdslen(args) < 128; i++)
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)c->argv[i]->ptr);
        addReplyErrorFormat(c,"unknown command `%s`, with args beginning with: %s",
            (char*)c->argv[0]->ptr, args);
        sdsfree(args);
        return C_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        flagTransaction(c);
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return C_OK;
    }

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && c->cmd->proc != authCommand)
    {
        flagTransaction(c);
        addReply(c,shared.noautherr);
        return C_OK;
    }

    /* If cluster is enabled perform the cluster redirection here.
     * However we don't perform the redirection if:
     * 1) The sender of this command is our master.
     * 2) The command has no key arguments. */
    if (server.cluster_enabled &&
        !(c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_LUA &&
          server.lua_caller->flags & CLIENT_MASTER) &&
        !(c->cmd->getkeys_proc == NULL && c->cmd->firstkey == 0 &&
          c->cmd->proc != execCommand))
    {
        int hashslot;
        int error_code;
        clusterNode *n = getNodeByQuery(c,c->cmd,c->argv,c->argc,
                                        &hashslot,&error_code);
        if (n == NULL || n != server.cluster->myself) {
            if (c->cmd->proc == execCommand) {
                discardTransaction(c);
            } else {
                flagTransaction(c);
            }
            clusterRedirectClient(c,n,hashslot,error_code);
            return C_OK;
        }
    }

    /* Handle the maxmemory directive.
     *
     * First we try to free some memory if possible (if there are volatile
     * keys in the dataset). If there are not the only thing we can do
     * is returning an error. */
    if (server.maxmemory) {
        int retval = freeMemoryIfNeeded();
        /* freeMemoryIfNeeded may flush slave output buffers. This may result
         * into a slave, that may be the active client, to be freed. */
        if (server.current_client == NULL) return C_ERR;

        /* It was impossible to free enough memory, and the command the client
         * is trying to execute is denied during OOM conditions? Error. */
        if ((c->cmd->flags & CMD_DENYOOM) && retval == C_ERR) {
            flagTransaction(c);
            addReply(c, shared.oomerr);
            return C_OK;
        }
    }

    /* Don't accept write commands if there are problems persisting on disk
     * and if this is a master instance. */
    int deny_write_type = writeCommandsDeniedByDiskError();
    if (deny_write_type != DISK_ERROR_TYPE_NONE &&
        server.masterhost == NULL &&
        (c->cmd->flags & CMD_WRITE ||
         c->cmd->proc == pingCommand))
    {
        flagTransaction(c);
        if (deny_write_type == DISK_ERROR_TYPE_RDB)
            addReply(c, shared.bgsaveerr);
        else
            addReplySds(c,
                sdscatprintf(sdsempty(),
                "-MISCONF Errors writing to the AOF file: %s\r\n",
                strerror(server.aof_last_write_errno)));
        return C_OK;
    }

  
  /* Don't accept write commands if there are not enough good slaves and
     * user configured the min-slaves-to-write option. */
    if (server.masterhost == NULL &&
        server.repl_min_slaves_to_write &&
        server.repl_min_slaves_max_lag &&
        c->cmd->flags & CMD_WRITE &&
        server.repl_good_slaves_count < server.repl_min_slaves_to_write)
    {
        flagTransaction(c);
        addReply(c, shared.noreplicaserr);
        return C_OK;
    }

    /* Don't accept write commands if this is a read only slave. But
     * accept write commands if this is our master. */
    if (server.masterhost && server.repl_slave_ro &&
        !(c->flags & CLIENT_MASTER) &&
        c->cmd->flags & CMD_WRITE)
    {
        addReply(c, shared.roslaveerr);
        return C_OK;
    }

    /* Only allow SUBSCRIBE and UNSUBSCRIBE in the context of Pub/Sub */
    if (c->flags & CLIENT_PUBSUB &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand) {
        addReplyError(c,"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
        return C_OK;
    }

    /* Only allow commands with flag "t", such as INFO, SLAVEOF and so on,
     * when slave-serve-stale-data is no and we are a slave with a broken
     * link with master. */
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED &&
        server.repl_serve_stale_data == 0 &&
        !(c->cmd->flags & CMD_STALE))
    {
        flagTransaction(c);
        addReply(c, shared.masterdownerr);
        return C_OK;
    }

    /* Loading DB? Return an error if the command has not the
     * CMD_LOADING flag. */
    if (server.loading && !(c->cmd->flags & CMD_LOADING)) {
        addReply(c, shared.loadingerr);
        return C_OK;
    }

    /* Lua script too slow? Only allow a limited number of commands. */
    if (server.lua_timedout &&
          c->cmd->proc != authCommand &&
          c->cmd->proc != replconfCommand &&
        !(c->cmd->proc == shutdownCommand &&
          c->argc == 2 &&
          tolower(((char*)c->argv[1]->ptr)[0]) == 'n') &&
        !(c->cmd->proc == scriptCommand &&
          c->argc == 2 &&
          tolower(((char*)c->argv[1]->ptr)[0]) == 'k'))
    {
        flagTransaction(c);
        addReply(c, shared.slowscripterr);
        return C_OK;
    }



	return 	sendToCombiner(combiner, c);

}

int sendToCombiner(reCombinerInfo *combiner, client *clientData)
{
	int res = 0;
	if(combiner->channel != NULL) {
		char *combinerChannel = combiner->channel;
		if(!channelIsCreate(combinerChannel)){
			res = createChannel(combinerChannel);
		}

		int writeMode = WR_BLOCK;
		int fdWorkerToCombiner = openChannel(combinerChannel, writeMode);
		writeChannel(fdWorkerToCombiner, clientData);
		close(fdWorkerToCombiner);
	} else {
		// we can't be here
		serverLog(LL_WARNING,"can't send to combiner, cause the combiner channel is null");
		res = -1;
	}
	return res;
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
	reWorkerInfo *worker = (reWorkerInfo *)value;
    list *l = worker->workerList;
	if(worker->channel == NULL) {
		worker->channel = (char *)zmalloc(sizeof(char)*CHANNEL_LEN);
		sprintf(worker->channel,"%s_%ld", WORKER, worker->workerId);
	}
	char *workerChannel = worker->channel;
	int run = worker->workerRun;
    if(!channelIsCreate(workerChannel)){
        createChannel(workerChannel);
    }
    
    int readMode = RD_NOBLOCK;
    int fdPrimaryToWorker = openChannel(workerChannel, readMode);
    int readlen = 0;
    while(run) {
        while(l->len == 0 || readlen > 0) {
            readlen = readChannel(fdPrimaryToWorker, l);
            sleep(1);
        }
        while(l->len > 0) {
            listNode *node = listPop(l);
			client * clientData = (client *)turnToAddr(node->value);

			
			processInputBuffer(clientData);
            zfree(node);
        }
    }
    
    close(fdPrimaryToWorker);
    return NULL;
}

void beforeSleepRE(struct aeEventLoop *eventLoop)
{
/*
 * we do nothing here, combiner thread will do it.
 */

	UNUSED(eventLoop);
}

int execCommandRE(void *value)
{
	client *c = (client*)value;
	if (c->flags & CLIENT_MULTI &&
        c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
        c->cmd->proc != multiCommand && c->cmd->proc != watchCommand)
    {
        queueMultiCommand(c);
        addReply(c,shared.queued);
    } else {
        call(c,CMD_CALL_FULL);
        c->woff = server.master_repl_offset;
        if (listLength(server.ready_keys))
            handleClientsBlockedOnKeys();
    }
    return C_OK;
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
    if(!channelIsCreate(channel)){
         createChannel(channel);
    }
	int run = combiner->combinerRun;
    int readMode = RD_NOBLOCK;
    int pipefd = openChannel(channel, readMode);
    int readlen = 0;
    while(run) {
        while(l->len == 0 || readlen > 0) {
            readlen = readChannel(pipefd, l);
            //sleep(1);
        }
        while(l->len > 0) {
            listNode *node = listPop(l);
			client *tmpClient = (client *)turnToAddr(node->value);
			if(strcmp("LOADRE", (char*)tmpClient->argv[0]->ptr) != 0) {
				execCommandRE(tmpClient);
			} else {
				addReply(tmpClient,shared.ok);
			}
			if (tmpClient->flags & CLIENT_MASTER && !(tmpClient->flags & CLIENT_MULTI)) {
                tmpClient->reploff = tmpClient->read_reploff - sdslen(tmpClient->querybuf);
            }

            if (!(tmpClient->flags & CLIENT_BLOCKED) || tmpClient->btype != BLOCKED_MODULE)
                resetClient(tmpClient);
            zfree(node);
        }
		standardBeforeSleep(server.el);
    }
    return NULL;
}
void loadRedisExtern(client *c)
{

	int workerSize = atoi((char *)c->argv[1]->ptr);
	combiner = combinerInit(); 
	workerTable = workerInit(workerSize);	
	aeBeforeSleepHook = beforeSleepRE;
	processInputBufferProcHook = processInputBufferRE;
    processComandProcREHook = processCommandRE;
	sleep(1);
	sendToCombiner(combiner, c);	
}

void unloadRedisExtern()
{
	int i;
	processInputBufferProcHook = NULL;
	processComandProcREHook = NULL;
	for(i = 0; i < workerTable->currWorkers; i++) {
		workerTable->workers[i].workerRun = 0;
	}

	while(combiner->combinerList->len != 0) {
		sleep(1);
	}

	combiner->combinerRun = 0;

	aeBeforeSleepHook = NULL;
}
