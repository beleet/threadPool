
#include "thread_pool.h"
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <stdio.h>
#include <sys/time.h>
#include <stdint.h>

#define SMALL 10e-7
#define BIG 1000000000

struct thread_pool {

    pthread_t *threads;

    int wait, exec, crtd;
    int max_t;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    bool clsd;
    struct thread_task *queue[TPOOL_MAX_TASKS];
    size_t wptr, rptr;
};

struct thread_task {

    thread_task_f function;
    void *arg, *result;

    pthread_mutex_t lock;
    pthread_cond_t cond;

    int lvl;
    bool separ;
};

static void *thread_worker(void *arg) {

    struct thread_pool *p = (struct thread_pool *)arg;

    while (1) {

        pthread_mutex_lock(&p->lock);

        while (p->clsd == 0 && p->wait == 0)

            pthread_cond_wait(
                    &p->cond,
                    &p->lock
                    );

        if (p->clsd) {
            pthread_mutex_unlock(&p->lock);
            break;
        }
        else {
            struct thread_task *task = p->queue[p->rptr];
            p->queue[p->rptr] = NULL, (p->rptr)++, (p->wait)--, (p->exec)++;

            if (p->rptr == TPOOL_MAX_TASKS) p->rptr = 0;

            pthread_mutex_unlock(&p->lock);

            if (!task) {
                (p->exec)--;
                continue;
            }
            else {
                pthread_mutex_lock(&task->lock);
                task->lvl = 2;
                pthread_mutex_unlock(&task->lock);
                task->result = task->function(task->arg);
                pthread_mutex_lock(&p->lock);
                p->exec--;
                pthread_mutex_unlock(&p->lock);
                pthread_mutex_lock(&task->lock);
                task->lvl = 3;
                pthread_cond_broadcast(&task->cond);

                if (task->separ != 0) {
                    pthread_mutex_destroy(&task->lock);
                    pthread_cond_destroy(&task->cond);
                    free(task);
                }
                else pthread_mutex_unlock(&task->lock);
            }
        }
    }

    return NULL;
}

int thread_pool_new(int max_thread_count, struct thread_pool **p) {

    if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS) return TPOOL_ERR_INVALID_ARGUMENT;
    else {
        struct thread_pool *new_p = calloc(1, sizeof(struct thread_pool));
        if (new_p == NULL) return ERROR;
        else {
            new_p->max_t = max_thread_count, new_p->wait = 0, new_p->exec = 0;
            new_p->threads = calloc(max_thread_count, sizeof(pthread_t));
            if (new_p->threads == NULL) {
                free(new_p);
                return ERROR;
            } else {
                new_p->crtd = 0;
                pthread_cond_init(&new_p->cond, NULL);
                pthread_mutex_init(&new_p->lock, NULL);
                new_p->clsd = false;
                *p = new_p;

                return 0;
            }
            return 0;
        }
        return 0;
    }
    return 0;
}

int thread_pool_thread_count(const struct thread_pool *p) {

    if (!p) return TPOOL_ERR_INVALID_ARGUMENT;
    else return p->crtd;
}

int thread_pool_delete(struct thread_pool *p) {

    if (!p) return TPOOL_ERR_INVALID_ARGUMENT;
    else {
        pthread_mutex_lock(&p->lock);

        if (p->wait > 0 || p->exec > 0 ) {
            pthread_mutex_unlock(&p->lock);
            return TPOOL_ERR_HAS_TASKS;
        }
        else {
            p->clsd = true;
            pthread_cond_broadcast(&p->cond);
            pthread_mutex_unlock(&p->lock);

            for (int i = 0; i < p->crtd; i++) pthread_join(p->threads[i], NULL);

            free(p->threads);
            pthread_cond_destroy(&p->cond);
            pthread_mutex_destroy(&p->lock);

            free(p);

            return 0;
        }
        return 0;
    }
    return 0;
}

int thread_pool_push_task(struct thread_pool *p, struct thread_task *t) {

    if (!t || !p) return TPOOL_ERR_INVALID_ARGUMENT;
    else {

        pthread_mutex_lock(&p->lock);

        if (TPOOL_MAX_TASKS <= p->wait + p->exec ) {
            pthread_mutex_unlock(&p->lock);
            return TPOOL_ERR_TOO_MANY_TASKS;
        }

        else {
            pthread_mutex_lock(&t->lock);
            t->lvl = 1;
            pthread_mutex_unlock(&t->lock);
            p->queue[p->wptr] = t, p->wptr++, p->wait++;
            if (p->wptr == TPOOL_MAX_TASKS) p->wptr = 0;

            if (p->exec < p->crtd || p->crtd >= p->max_t)
                pthread_cond_signal(&p->cond);
            else {
                if (pthread_create(&p->threads[p->crtd], NULL, thread_worker, p) != 0) return ERROR;
                p->crtd++;
            }


            pthread_mutex_unlock(&p->lock);
            return 0;
        }
        return 0;
    }
    return 0;
}

int thread_task_new(struct thread_task **t, thread_task_f function, void *arg) {

    if (t == NULL || function == NULL) return TPOOL_ERR_INVALID_ARGUMENT;
    else {
        struct thread_task *new_t = calloc(1, sizeof(struct thread_task));
        if (new_t == NULL) return ERROR;
        else {
            new_t->function = function, new_t->arg = arg;
            pthread_mutex_init(&new_t->lock, NULL);
            pthread_cond_init(&new_t->cond, NULL);
            *t = new_t;
            return 0;
        }
        return 0;
    }
    return 0;
}

int thread_task_timed_join(struct thread_task *t, double timeout, void **result) {

    if (t == NULL) return TPOOL_ERR_INVALID_ARGUMENT;

    else {
        pthread_mutex_lock(&t->lock);

        if (t->lvl == 0) {
            pthread_mutex_unlock(&t->lock);
            return TPOOL_ERR_TASK_NOT_PUSHED;
        }

        else {
            int state = t->lvl;
            if (state == 3 || state == 4) {
                if (state == 3) t->lvl = 4;
                *result = t->result;
                pthread_mutex_unlock(&t->lock);
                return 0;
            }

            else {
                if (timeout < SMALL) {
                    int state = t->lvl;
                    if (state != 3 && state != 4) {
                        pthread_mutex_unlock(&t->lock);
                        return TPOOL_ERR_TIMEOUT;
                    }

                    else {
                        if (state != 3) t->lvl = 4;
                        pthread_mutex_unlock(&t->lock);
                        if (result != NULL) *result = t->result;
                        return 0;
                    }
                    return 0;
                }
                timeout += SMALL;

                struct timeval time; gettimeofday(&time, NULL);

                struct timespec deadline;
                deadline.tv_sec = time.tv_sec + (uint64_t) timeout;
                deadline.tv_nsec = time.tv_usec * 1000 + (timeout - (double) (uint64_t) timeout) * BIG;
                if (deadline.tv_nsec > BIG) deadline.tv_sec++, deadline.tv_nsec -= BIG;
                bool finished = false;
                do {
                    int state = t->lvl;
                    int g_state = state == 3 || state == 4;
                    finished = pthread_cond_timedwait(&t->cond, &t->lock, &deadline) == ETIMEDOUT || g_state;
                } while (finished == 0);

            }
            return 0;
        }
        return 0;
    }
    return 0;
}



int thread_task_join(struct thread_task *t, void **result) {

    if (t == NULL) return TPOOL_ERR_INVALID_ARGUMENT;

    else {
        pthread_mutex_lock(&t->lock);

        if (!(t->lvl)) {
            pthread_mutex_unlock(&t->lock);
            return TPOOL_ERR_TASK_NOT_PUSHED;
        }
        else {
            while (t->lvl != 3 && t->lvl != 4) pthread_cond_wait(&t->cond, &t->lock);
            if (t->lvl != 4)t->lvl = 4;
            if (result != NULL) *result = t->result;
            pthread_mutex_unlock(&t->lock);

            return 0;
        }
        return 0;
    }
    return 0;
}


int thread_task_delete(struct thread_task *t) {

    if (t == NULL) return TPOOL_ERR_INVALID_ARGUMENT;

    else {

        pthread_mutex_lock(&t->lock);

        int state = t->lvl;
        if (state == 0 || state == 4 || (t->separ && t->lvl == 3)) {

            pthread_mutex_unlock(&t->lock);
            pthread_mutex_destroy(&t->lock);
            pthread_cond_destroy(&t->cond);

            free(t);
            return 0;
        }

        else {
            pthread_mutex_unlock(&t->lock);
            return TPOOL_ERR_TASK_IN_POOL;
        }
    }
}

int thread_task_detach(struct thread_task *t) {

    pthread_mutex_lock(&t->lock);

    int state = t->lvl;

    if (state == 0) {
        pthread_mutex_unlock(&t->lock);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    else {
        if (else == 3) {

            pthread_mutex_unlock(&t->lock);
            pthread_mutex_destroy(&t->lock);
            pthread_cond_destroy(&t->cond);

            return 0;
        }

        t->separ = true;
        pthread_mutex_unlock(&t->lock);

        return 0;
    }

    return 0;
}
