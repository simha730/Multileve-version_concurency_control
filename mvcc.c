#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#define MAX_KEYS 64
#define MAX_KEYNAME 32
#define MAX_TRANSACTIONS 128
#define MAX_READSET 64
#define ACQUIRE_RETRY_US 20000

typedef int txid_t;
typedef int commit_ts_t;

typedef struct Version {
    commit_ts_t commit_ts;
    txid_t tx_owner;
    char *value;
    struct Version *next;
} Version;

typedef struct Key {
    char name[MAX_KEYNAME];
    Version *versions;
    txid_t lock_owner;
} Key;

typedef enum {TX_ACTIVE, TX_ABORTED, TX_COMMITTED} tx_state_t;

typedef struct Transaction {
    txid_t id;
    commit_ts_t start_ts;
    tx_state_t state;
    char read_set[MAX_READSET][MAX_KEYNAME];
    int read_count;
    char write_set_keys[MAX_READSET][MAX_KEYNAME];
    char write_set_vals[MAX_READSET][128];
    int write_count;
} Transaction;

Key store[MAX_KEYS];
int store_count = 0;
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;
commit_ts_t global_commit_ts = 1;
txid_t global_tx_seq = 1;
int wait_for[MAX_TRANSACTIONS+1][MAX_TRANSACTIONS+1];
Transaction *tx_table[MAX_TRANSACTIONS+1];

Key *get_key(const char *k) {
    for (int i=0;i<store_count;i++) {
        if (strcmp(store[i].name, k) == 0) return &store[i];
    }
    return NULL;
}

Key *create_key(const char *k, const char *initial) {
    if (store_count >= MAX_KEYS) return NULL;
    Key *key = &store[store_count++];
    strncpy(key->name, k, MAX_KEYNAME-1);
    key->name[MAX_KEYNAME-1] = 0;
    key->lock_owner = 0;
    Version *v = malloc(sizeof(Version));
    v->commit_ts = 1;
    v->tx_owner = 0;
    v->value = strdup(initial ? initial : "");
    v->next = NULL;
    key->versions = v;
    return key;
}

void add_wait_edge(txid_t a, txid_t b) {
    if (a<=0 || b<=0 || a>MAX_TRANSACTIONS || b>MAX_TRANSACTIONS) return;
    wait_for[a][b] = 1;
}

void remove_wait_edges_of(txid_t a) {
    if (a<=0) return;
    for (int i=0;i<=MAX_TRANSACTIONS;i++) wait_for[a][i]=0;
    for (int i=0;i<=MAX_TRANSACTIONS;i++) wait_for[i][a]=0;
}

int dfs_cycle(int node, int visited[], int stack[]) {
    visited[node]=1; stack[node]=1;
    for (int j=1;j<=MAX_TRANSACTIONS;j++) {
        if (wait_for[node][j]) {
            if (!visited[j]) {
                if (dfs_cycle(j,visited,stack)) return 1;
            } else if (stack[j]) return 1;
        }
    }
    stack[node]=0;
    return 0;
}

int detect_deadlock() {
    int visited[MAX_TRANSACTIONS+1]={0};
    int stack[MAX_TRANSACTIONS+1]={0};
    for (int i=1;i<=MAX_TRANSACTIONS;i++) {
        if (tx_table[i] && !visited[i]) {
            if (dfs_cycle(i,visited,stack)) return 1;
        }
    }
    return 0;
}

const char *mvcc_read(Transaction *tx, Key *key) {
    Version *v = key->versions;
    while (v) {
        if (v->commit_ts > 0 && v->commit_ts <= tx->start_ts) return v->value;
        if (v->commit_ts == 0 && v->tx_owner == tx->id) return v->value;
        v = v->next;
    }
    return NULL;
}

Transaction *tx_begin() {
    pthread_mutex_lock(&global_lock);
    txid_t id = global_tx_seq++;
    Transaction *tx = calloc(1,sizeof(Transaction));
    tx->id = id;
    tx->start_ts = global_commit_ts;
    tx->state = TX_ACTIVE;
    tx_table[id] = tx;
    pthread_mutex_unlock(&global_lock);
    printf("[TX %d] BEGIN (snapshot ts=%d)\n", id, tx->start_ts);
    return tx;
}

void record_read(Transaction *tx, const char *key) {
    if (tx->read_count < MAX_READSET) strncpy(tx->read_set[tx->read_count++], key, MAX_KEYNAME-1);
}

void record_write_buffer(Transaction *tx, const char *key, const char *val) {
    if (tx->write_count < MAX_READSET) {
        strncpy(tx->write_set_keys[tx->write_count], key, MAX_KEYNAME-1);
        strncpy(tx->write_set_vals[tx->write_count], val, 127);
        tx->write_count++;
    }
}

int acquire_key_lock(txid_t tid, const char *keyname) {
    while (1) {
        pthread_mutex_lock(&global_lock);
        Key *k = get_key(keyname);
        if (!k) { pthread_mutex_unlock(&global_lock); return -1; }
        if (k->lock_owner == 0) {
            k->lock_owner = tid;
            remove_wait_edges_of(tid);
            pthread_mutex_unlock(&global_lock);
            return 0;
        } else if (k->lock_owner == tid) {
            pthread_mutex_unlock(&global_lock);
            return 0;
        } else {
            add_wait_edge(tid, k->lock_owner);
            int dead = detect_deadlock();
            if (dead) {
                remove_wait_edges_of(tid);
                pthread_mutex_unlock(&global_lock);
                printf("[TX %d] DEADLOCK detected while waiting for %s (owner TX %d). Aborting.\n", tid, keyname, k->lock_owner);
                return -1;
            }
            pthread_mutex_unlock(&global_lock);
            usleep(ACQUIRE_RETRY_US);
            continue;
        }
    }
}

void release_locks(txid_t tid) {
    pthread_mutex_lock(&global_lock);
    for (int i=0;i<store_count;i++) if (store[i].lock_owner == tid) store[i].lock_owner = 0;
    remove_wait_edges_of(tid);
    pthread_mutex_unlock(&global_lock);
}

void tx_read(Transaction *tx, const char *keyname) {
    if (!tx || tx->state != TX_ACTIVE) return;
    pthread_mutex_lock(&global_lock);
    Key *k = get_key(keyname);
    const char *v = NULL;
    if (k) v = mvcc_read(tx, k);
    pthread_mutex_unlock(&global_lock);
    printf("[TX %d] READ %s -> %s\n", tx->id, keyname, v?v:"(null)");
    record_read(tx, keyname);
}

int tx_write(Transaction *tx, const char *keyname, const char *value) {
    if (!tx || tx->state != TX_ACTIVE) return -1;
    if (acquire_key_lock(tx->id, keyname) != 0) {
        tx->state = TX_ABORTED;
        return -1;
    }
    pthread_mutex_lock(&global_lock);
    Key *k = get_key(keyname);
    if (!k) k = create_key(keyname,"");
    Version *v = malloc(sizeof(Version));
    v->commit_ts = 0;
    v->tx_owner = tx->id;
    v->value = strdup(value);
    v->next = k->versions;
    k->versions = v;
    pthread_mutex_unlock(&global_lock);
    record_write_buffer(tx, keyname, value);
    printf("[TX %d] WRITE %s = %s (uncommitted)\n", tx->id, keyname, value);
    return 0;
}

int check_read_write_conflicts(Transaction *tx) {
    for (int i=0;i<tx->read_count;i++) {
        Key *k = get_key(tx->read_set[i]);
        if (!k) continue;
        Version *v = k->versions;
        if (v && v->commit_ts > tx->start_ts) {
            printf("[TX %d] ABORT due to read-write conflict on %s (latest ts=%d > start=%d)\n", tx->id, k->name, v->commit_ts, tx->start_ts);
            return -1;
        }
    }
    return 0;
}

int tx_commit(Transaction *tx) {
    if (!tx || tx->state != TX_ACTIVE) return -1;
    for (int i=0;i<tx->write_count;i++) {
        if (acquire_key_lock(tx->id, tx->write_set_keys[i]) != 0) {
            tx->state = TX_ABORTED;
            release_locks(tx->id);
            printf("[TX %d] ABORT during lock acquisition\n", tx->id);
            return -1;
        }
    }
    pthread_mutex_lock(&global_lock);
    if (check_read_write_conflicts(tx) != 0) {
        pthread_mutex_unlock(&global_lock);
        tx->state = TX_ABORTED;
        release_locks(tx->id);
        return -1;
    }
    for (int i=0;i<store_count;i++) {
        Key *k = &store[i];
        Version *v = k->versions;
        while (v) {
            if (v->commit_ts == 0 && v->tx_owner == tx->id) {
                v->commit_ts = ++global_commit_ts;
                v->tx_owner = 0;
                printf("[TX %d] COMMITTED %s = %s (ts=%d)\n", tx->id, k->name, v->value, v->commit_ts);
            }
            v = v->next;
        }
    }
    tx->state = TX_COMMITTED;
    pthread_mutex_unlock(&global_lock);
    release_locks(tx->id);
    return 0;
}

void tx_abort(Transaction *tx) {
    if (!tx) return;
    pthread_mutex_lock(&global_lock);
    for (int i=0;i<store_count;i++) {
        Key *k = &store[i];
        Version **prev = &k->versions;
        Version *v = k->versions;
        while (v) {
            if (v->commit_ts == 0 && v->tx_owner == tx->id) {
                *prev = v->next;
                free(v->value);
                free(v);
                v = *prev;
            } else {
                prev = &v->next;
                v = v->next;
            }
        }
    }
    pthread_mutex_unlock(&global_lock);
    release_locks(tx->id);
    tx->state = TX_ABORTED;
    printf("[TX %d] ABORTED\n", tx->id);
}

typedef struct WorkerArgs { const char *k1; const char *v1; const char *k2; const char *v2; int sleep_ms; } WorkerArgs;

void *worker_fn(void *arg) {
    WorkerArgs *a = arg;
    Transaction *tx = tx_begin();
    tx_read(tx, a->k1);
    tx_write(tx, a->k1, a->v1);
    usleep(a->sleep_ms * 1000);
    tx_write(tx, a->k2, a->v2);
    if (tx_commit(tx) == 0) printf("[TX %d] COMMIT SUCCESS\n", tx->id);
    else { tx_abort(tx); printf("[TX %d] COMMIT FAILED\n", tx->id); }
    return NULL;
}

int main() {
    create_key("A","initialA");
    create_key("B","initialB");
    printf("=== MVCC + Locks + Deadlock demo ===\n");
    pthread_t t1,t2;
    WorkerArgs a1 = {"A","v1_from_tx1","B","v2_from_tx1",200};
    WorkerArgs a2 = {"B","v1_from_tx2","A","v2_from_tx2",50};
    pthread_create(&t1,NULL,worker_fn,&a1);
    pthread_create(&t2,NULL,worker_fn,&a2);
    pthread_join(t1,NULL);
    pthread_join(t2,NULL);
    printf("\nFinal snapshot reads by new tx:\n");
    Transaction *tx = tx_begin();
    tx_read(tx,"A");
    tx_read(tx,"B");
    return 0;
}
