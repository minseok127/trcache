#ifndef TRCACHE_THREAD_COMPAT_H
#define TRCACHE_THREAD_COMPAT_H

/**
 * @file    compat/thread_compat.h
 * @brief   pthread -> Win32 compatibility layer.
 *
 * On Linux this simply includes <pthread.h>.
 * On Windows (MSVC) it provides inline wrappers around Win32 primitives
 * so that all .c files can keep using pthread_* names unchanged.
 */

#ifdef _WIN32
/* ------------------------------------------------------------------ */
/*  Windows path                                                      */
/* ------------------------------------------------------------------ */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <stdlib.h>  /* for abort() */

/* ---- types ------------------------------------------------------- */

typedef HANDLE pthread_t;
typedef DWORD  pthread_key_t;

typedef CRITICAL_SECTION pthread_mutex_t;
typedef CRITICAL_SECTION pthread_spinlock_t;

/*
 * pthread_once_t: We piggy-back on INIT_ONCE which is exactly
 * the same one-shot pattern.
 */
typedef INIT_ONCE pthread_once_t;
#define PTHREAD_ONCE_INIT INIT_ONCE_STATIC_INIT

/* Unused on Windows, accepted for API compat. */
#define PTHREAD_PROCESS_PRIVATE 0

/* ---- pthread_create / join --------------------------------------- */

typedef struct {
	void *(*start_routine)(void *);
	void *arg;
} trc_thread_trampoline_ctx;

static inline DWORD WINAPI trc_thread_trampoline_(LPVOID p)
{
	trc_thread_trampoline_ctx ctx = *(trc_thread_trampoline_ctx *)p;
	free(p);
	ctx.start_routine(ctx.arg);
	return 0;
}

static inline int pthread_create(pthread_t *t, const void *attr,
	void *(*start)(void *), void *arg)
{
	trc_thread_trampoline_ctx *ctx;
	(void)attr;
	ctx = (trc_thread_trampoline_ctx *)malloc(sizeof(*ctx));
	if (ctx == NULL) {
		return -1;
	}
	ctx->start_routine = start;
	ctx->arg = arg;
	*t = CreateThread(NULL, 0, trc_thread_trampoline_, ctx, 0, NULL);
	if (*t == NULL) {
		free(ctx);
		return -1;
	}
	return 0;
}

static inline int pthread_join(pthread_t t, void **retval)
{
	(void)retval;
	WaitForSingleObject(t, INFINITE);
	CloseHandle(t);
	return 0;
}

/* ---- mutex ------------------------------------------------------- */

static inline int pthread_mutex_init(pthread_mutex_t *m,
	const void *attr)
{
	(void)attr;
	InitializeCriticalSection(m);
	return 0;
}

static inline int pthread_mutex_lock(pthread_mutex_t *m)
{
	EnterCriticalSection(m);
	return 0;
}

static inline int pthread_mutex_unlock(pthread_mutex_t *m)
{
	LeaveCriticalSection(m);
	return 0;
}

static inline int pthread_mutex_destroy(pthread_mutex_t *m)
{
	DeleteCriticalSection(m);
	return 0;
}

/* ---- spinlock (emulated via CRITICAL_SECTION) -------------------- */

static inline int pthread_spin_init(pthread_spinlock_t *s, int pshared)
{
	(void)pshared;
	InitializeCriticalSectionAndSpinCount(s, 4000);
	return 0;
}

static inline int pthread_spin_lock(pthread_spinlock_t *s)
{
	EnterCriticalSection(s);
	return 0;
}

static inline int pthread_spin_unlock(pthread_spinlock_t *s)
{
	LeaveCriticalSection(s);
	return 0;
}

static inline int pthread_spin_destroy(pthread_spinlock_t *s)
{
	DeleteCriticalSection(s);
	return 0;
}

/* ---- TLS (Thread-Local Storage) ---------------------------------- */

static inline int pthread_key_create(pthread_key_t *key,
	void (*destructor)(void *))
{
	/*
	 * Win32 TlsAlloc does not support per-key destructors.
	 * For trcache this is acceptable: the atomsnap and scq TLS
	 * destructors are only needed for reclamation on thread exit,
	 * which can be handled via DllMain or explicit cleanup on Windows.
	 */
	(void)destructor;
	*key = TlsAlloc();
	return (*key == TLS_OUT_OF_INDEXES) ? -1 : 0;
}

static inline void *pthread_getspecific(pthread_key_t key)
{
	return TlsGetValue(key);
}

static inline int pthread_setspecific(pthread_key_t key, const void *val)
{
	return TlsSetValue(key, (LPVOID)val) ? 0 : -1;
}

static inline int pthread_key_delete(pthread_key_t key)
{
	return TlsFree(key) ? 0 : -1;
}

/* ---- pthread_once ------------------------------------------------ */

typedef struct {
	void (*init_routine)(void);
} trc_once_ctx;

static inline BOOL CALLBACK trc_once_callback_(
	PINIT_ONCE once, PVOID param, PVOID *ctx)
{
	(void)once;
	(void)ctx;
	trc_once_ctx *c = (trc_once_ctx *)param;
	c->init_routine();
	return TRUE;
}

static inline int pthread_once(pthread_once_t *once,
	void (*init_routine)(void))
{
	trc_once_ctx ctx;
	ctx.init_routine = init_routine;
	InitOnceExecuteOnce(once, trc_once_callback_, &ctx, NULL);
	return 0;
}

#else
/* ------------------------------------------------------------------ */
/*  Linux / POSIX path                                                */
/* ------------------------------------------------------------------ */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <pthread.h>
#endif /* _WIN32 */

#endif /* TRCACHE_THREAD_COMPAT_H */
