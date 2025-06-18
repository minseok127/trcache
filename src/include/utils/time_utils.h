#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#include <stdint.h>
#include <time.h>
#include <stdio.h>
#include <string.h>

static inline void format_timestamp_ms(uint64_t ts_ms, char *buf, size_t buf_len)
{
	time_t secs = (time_t)(ts_ms / 1000ULL);
	int ms = (int)(ts_ms % 1000ULL);
	struct tm tm;
	gmtime_r(&secs, &tm);
	strftime(buf, buf_len, "%Y-%m-%d %H:%M:%S", &tm);
	size_t len = strlen(buf);
	if (len < buf_len) {
		snprintf(buf + len, buf_len - len, ".%03d", ms);
	}
}

#endif /* TIME_UTILS_H */
