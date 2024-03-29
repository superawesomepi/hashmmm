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
  ts_hashmap_t *newmap = (ts_hashmap_t*) malloc(sizeof(ts_hashmap_t));
  newmap->table = (ts_entry_t**) malloc(capacity * sizeof(ts_entry_t*));
  locks = (pthread_mutex_t**) malloc(capacity * sizeof(pthread_mutex_t*));
  for (int i = 0; i < capacity; i++) {
    newmap->table[i] = (ts_entry_t*) malloc(sizeof(ts_entry_t));
    locks[i] = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(locks[i], NULL);
  }
  newmap->capacity = capacity;
  return newmap;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  map->numOps++;
  int here = hashCode(map, key);
  pthread_mutex_lock(locks[here]);
  struct ts_entry_t* current = map->table[here];
  while(current != NULL) {
    if(current->key == key) return current->value;
    current = current->next;
  }
  pthread_mutex_unlock(locks[here]);
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
  map->numOps++;
  int here = hashCode(map, key);
  // check if key is already present
  struct ts_entry_t* current = map->table[here];
  while(current != NULL) {
    if(current->key == key) {
      int oldval = current->value;
      current->value = value;
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
  map->size++;
  return INT_MAX;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  map->numOps++;
  int here = hashCode(map, key);
  struct ts_entry_t* current = map->table[here];
  if(current == NULL) return INT_MAX; // there was nothing here why are you trying to delete it?
  struct ts_entry_t* previous = NULL;
  while(current != NULL) {
    if(current->key == key) break;
    previous = current;
    current = current->next;
  }
  
  // key not found
  if(current == NULL) return INT_MAX;

  // key found
  if(previous == NULL) map->table[here] = current->next; // node is head
  else previous->next = current->next; // node is not head
  map->size--;
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
  // TODO: iterate through each list, free up all nodes
  // TODO: free the hash table
  // TODO: destroy locks
}

