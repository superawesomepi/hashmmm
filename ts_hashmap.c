#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap. 
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {
  ts_hashmap_t *map = (ts_hashmap_t*) malloc(sizeof(ts_hashmap_t));
  map->table = (ts_entry_t**) malloc(capacity * sizeof(ts_entry_t*));
  map->locks = (pthread_mutex_t**) malloc(capacity * sizeof(pthread_mutex_t*));
  map->sizelock = (pthread_spinlock_t*) malloc(sizeof(pthread_spinlock_t));
  pthread_spin_init(map->sizelock, PTHREAD_PROCESS_PRIVATE);
  map->opslock = (pthread_spinlock_t*) malloc(sizeof(pthread_spinlock_t));
  pthread_spin_init(map->opslock, PTHREAD_PROCESS_PRIVATE);
  for (int i = 0; i < capacity; i++) {
    map->table[i] = (ts_entry_t*) malloc(sizeof(ts_entry_t));
    map->locks[i] = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(map->locks[i], NULL);
  }
  map->capacity = capacity;
  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  pthread_spin_lock(map->opslock);
  map->numOps++;
  pthread_spin_unlock(map->opslock);
  int here = hashCode(map, key);
  pthread_mutex_lock(map->locks[here]);
  struct ts_entry_t* current = map->table[here];
  while(current != NULL) {
    if(current->key == key) {
      pthread_mutex_unlock(map->locks[here]);
      return current->value;
    }
    current = current->next;
  }
  pthread_mutex_unlock(map->locks[here]);
  return INT_MAX;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  pthread_spin_lock(map->opslock);
  map->numOps++;
  pthread_spin_unlock(map->opslock);
  int here = hashCode(map, key);
  // check if key is already present
  pthread_mutex_lock(map->locks[here]);
  struct ts_entry_t* current = map->table[here];
  while(current != NULL) {
    if(current->key == key) {
      int oldval = current->value;
      current->value = value;
      pthread_mutex_unlock(map->locks[here]);
      return oldval;
    }
    current = current->next;
  }
  // otherwise the key wasn't present, so add a new entry
  struct ts_entry_t* newEntry = (ts_entry_t*) malloc (sizeof(ts_entry_t));
  if(map->table[here] == NULL) map->table[here] = newEntry;
  else {
    newEntry->next = map->table[here];
    map->table[here] = newEntry;
  }
  newEntry->key = key;
  newEntry->value = value;
  pthread_mutex_unlock(map->locks[here]);
  pthread_spin_lock(map->sizelock);
  map->size++;
  pthread_spin_unlock(map->sizelock);
  return INT_MAX;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  pthread_spin_lock(map->opslock);
  map->numOps++;
  pthread_spin_unlock(map->opslock);
  int here = hashCode(map, key);
  pthread_mutex_lock(map->locks[here]);
  struct ts_entry_t* current = map->table[here];
  if(current == NULL) {
    pthread_mutex_unlock(map->locks[here]);
    return INT_MAX; // there was nothing here why are you trying to delete it?
  }
  struct ts_entry_t* previous = NULL;
  while(current != NULL) {
    if(current->key == key) break;
    previous = current;
    current = current->next;
  }
  
  // key not found
  if(current == NULL) {
    pthread_mutex_unlock(map->locks[here]);
    return INT_MAX;
  }

  // key found
  if(previous == NULL) map->table[here] = current->next; // node is head
  else previous->next = current->next; // node is not head
  pthread_mutex_unlock(map->locks[here]);
  pthread_spin_lock(map->sizelock);
  map->size--;
  pthread_spin_unlock(map->sizelock);
  return current->value;
}

int hashCode(ts_hashmap_t *map, int key) {
  unsigned int unskey = (unsigned int) key;
  return unskey % map->capacity;
}


/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
   // TODO: destroy locks
  pthread_spin_destroy(map->sizelock);
  free(map->sizelock); // valgrind doesn't recognize destroy I guess, or something weird is happening?
  pthread_spin_destroy(map->opslock);
  free(map->opslock); // valgrind doesn't recognize destroy
  // TODO: iterate through each list, free up all nodes
  for (int i = 0; i < map->capacity; i++) {
    struct ts_entry_t* current = map->table[i];
    struct ts_entry_t* temp = NULL;
    while(current != NULL) {
      temp = current->next;
      free(current);
      current = temp;
    }
    free(map->table[i]);
    // destroy the lock for each list in here as well
    pthread_mutex_destroy(map->locks[i]);
  }
  free(map->table);
  free(map->locks);
  // TODO: free the hash table
  free(map);
  

}

