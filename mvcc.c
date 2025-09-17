#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#define MAX_KEYS 32
#define MAX_KEYNAME 16
#define MAX_TRANSACTIONS 128
#define MAX_READSET 32

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
} Transaction;

Key store[MAX_KEYS];
int store_count = 0;
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;
commit_ts_t global_commit_ts = 1;
txid_t global_tx_seq = 1;
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

const char *mvcc_read(Transaction *tx, Key *key) {
    Version *v = key->versions;
    while (v) {
        if (v->commit_ts > 0 && v->commit_ts <= tx->start_ts)
            return v->value;
        if (v->commit_ts == 0 && v->tx_owner == tx->id)
            return v->value;
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

void tx_read(Transaction *tx, const char *keyname) {
    if (!tx || tx->state != TX_ACTIVE) return;
    pthread_mutex_lock(&global_lock);
    Key *k = get_key(keyname);
    const char *v = NULL;
    if (k) v = mvcc_read(tx, k);
    pthread_mutex_unlock(&global_lock);
    printf("[TX %d] READ %s -> %s\n", tx->id, keyname, v ? v : "(null)");
    if (tx->read_count < MAX_READSET)
        strncpy(tx->read_set[tx->read_count++], keyname, MAX_KEYNAME-1);
}

int tx_write(Transaction *tx, const char *keyname, const char *value) {
    if (!tx || tx->state != TX_ACTIVE) return -1;
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
    printf("[TX %d] WRITE %s = %s (uncommitted)\n", tx->id, keyname, value);
    return 0;
}

int tx_commit(Transaction *tx) {
    if (!tx || tx->state != TX_ACTIVE) return -1;
    pthread_mutex_lock(&global_lock);
    commit_ts_t new_ts = ++global_commit_ts;
    for (int i=0;i<store_count;i++) {
        Key *k = &store[i];
        Version *v = k->versions;
        while (v) {
            if (v->commit_ts == 0 && v->tx_owner == tx->id) {
                v->commit_ts = new_ts;
                v->tx_owner = 0;
                printf("[TX %d] COMMITTED %s = %s (ts=%d)\n", tx->id, k->name, v->value, new_ts);
            }
            v = v->next;
        }
    }
    tx->state = TX_COMMITTED;
    pthread_mutex_unlock(&global_lock);
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
    tx->state = TX_ABORTED;
    pthread_mutex_unlock(&global_lock);
    printf("[TX %d] ABORTED\n", tx->id);
}

int main() {
    create_key("A", "initialA");
    create_key("B", "initialB");

    Transaction *t1 = tx_begin();
    tx_read(t1, "A");
    tx_write(t1, "A", "val1");
    tx_read(t1, "A");
    tx_commit(t1);

    Transaction *t2 = tx_begin();
    tx_read(t2, "A");
    tx_write(t2, "B", "val2");
    tx_commit(t2);

    Transaction *t3 = tx_begin();
    tx_read(t3, "A");
    tx_read(t3, "B");

    return 0;
}

