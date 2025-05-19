#ifndef LOG_H
#define LOG_H

#include <stdio.h>

#define errmsg(stream, fmt, ...)                         \
	do {                                                 \
		fprintf((stream), "[%s:%d:%s] " fmt,             \
		__FILE__, __LINE__, __func__, ##__VA_ARGS__);    \
	} while (0)

#endif /* LOG_H */
