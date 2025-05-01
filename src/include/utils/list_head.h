#ifndef LIST_HEAD_H
#define LIST_HEAD_H

#include <stddef.h>

#ifndef container_of
#define container_of(ptr, type, member) \
	((type *)((char *)(ptr) - offsetof(type, member)))
#endif /* container_of */

/*
 * list_head - Node for a circular doubly-linked list.
 *
 * @next: Pointer to next entry in the list
 * @prev: Pointer to previous entry in the list
 *
 * Each list has a dummy head whose next points to the first element (or itself
 * if empty) and prev points to the last element (or itself).
 *
 * @note first is head of list and last is tail of list.
 */
struct list_head {
	struct list_head *next, *prev;
};

#define list_entry(ptr, type, member) \
	container_of(ptr, type, member)

/**
 * @brief	Static initializer for a list head.
 * @param	name:	Name of the list_head variable.
 */
#define LIST_HEAD_INIT(name) { &(name), &(name) }

/**
 * @brief	Define and initialize a list head variable.
 * @param	name:	Name of the list_head variable.
 */
#define LIST_HEAD(name) \
	struct list_head name = LIST_HEAD_INIT(name)

/**
 * @brief	Initialize a list head at runtime.
 * @param	list:	Pointer to list_head to initialize.
 */
static inline void INIT_LIST_HEAD(struct list_head *list)
{
	list->next = list;
	list->prev = list;
}

/**
 * @brief   Tests whether a list is empty.
 * @param   head: the list to test.
 * @return  1 on empty, 0 on non-empty.
 */
static inline int list_empty(const struct list_head *head)
{
	return head->next == head;
}

/**
 * @brief   Tests whether a entry is the first entry in head
 * @param   entry: the entry to test.
 * @param   head:  the list to test.
 * @return  1 on true, 0 on false.
 */
static inline int list_is_first(const struct list_head *entry,
	const struct list_head *head)
{
	return entry->prev == head;
}

/**
 * @brief   Tests whether a entry is the last entry in head
 * @param   entry: the entry to test.
 * @param   head:  the list to test.
 * @return  1 on true, 0 on false.
 */
static inline int list_is_last(const struct list_head *entry,
	const struct list_head *head)
{
	return entry->next == head;
}

/**
 * @brief   Get first entry of the list.
 * @param   head: the list to get the first entry.
 * @return  Pointer of the first entry.
 */
static inline struct list_head *list_get_first(
	const struct list_head *head)
{
	return head->next;
}

/**
 * @brief   Get last entry of the list.
 * @param   head: the list to get the last entry.
 * @return  Pointer of the last entry.
 */
static inline struct list_head *list_get_last(
	const struct list_head *head)
{
	return head->prev;
}

/**
 * @brief	Insert new entry between two consecutive entries.
 * @param	new:	New entry to add.
 * @param	prev:	Entry after which to insert.
 * @param	next:	Entry before which to insert.
 */
static inline void __list_add(struct list_head *new,
	struct list_head *prev, struct list_head *next)
{
	next->prev = new;
	new->next = next;
	new->prev = prev;
	prev->next = new;
}

/**
 * @brief	Add new entry immediately after the head.
 * @param	new:	New entry to add.
 * @param	head:	List head to add after.
 */
static inline void list_add(struct list_head *new, struct list_head *head)
{
	__list_add(new, head, head->next);
}

/**
 * @brief	Add new entry immediately before the head (tail position).
 * @param	new:	New entry to add.
 * @param	head:	List head to add before.
 */
static inline void list_add_tail(struct list_head *new, struct list_head *head)
{
	__list_add(new, head->prev, head);
}

/**
 * @brief	Delete an entry from the list.
 * @param	entry:	Entry to delete.
 */
static inline void list_del(struct list_head *entry)
{
	entry->next->prev = entry->prev;
	entry->prev->next = entry->next;
}

/**
 * @brief	Move an entry to the front (after head) of another list.
 * @param	entry:	Entry to move.
 * @param	head:	List head to move entry after.
 */
static inline void list_move(struct list_head *entry, struct list_head *head)
{
	list_del(entry);
	list_add(entry, head);
}

/**
 * @brief	Move an entry to the end (before head) of another list.
 * @param	entry:	Entry to move.
 * @param	head:	List head whose tail to insert before.
 */
static inline void list_move_tail(struct list_head *entry,
	struct list_head *head)
{
	list_del(entry);
	list_add_tail(entry, head);
}

/**
 * @brief	Move a contiguous sublist to the tail position.
 * @param	head:	List head to which to move.
 * @param	first:	First entry of the sublist.
 * @param	last:	Last entry of the sublist.
 */
static inline void list_bulk_move_tail(struct list_head *head,
	struct list_head *first, struct list_head *last)
{
	first->prev->next = last->next;
	last->next->prev = first->prev;

	head->prev->next = first;
	first->prev = head->prev;

	last->next = head;
	head->prev = last;
}

#endif /* LIST_HEAD_H */
