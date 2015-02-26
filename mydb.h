
#include <stddef.h>


struct DBC {
	/* Maximum on-disk file size
	 * 512MB by default
	 * */
	size_t db_size;
	/* Page (node/data) size
	 * 4KB by default
	 * */
	size_t page_size;
	/* Maximum cached memory size
	 * 16MB by default
	 * */
	size_t cache_size;
};


struct DB *dbopen (char *file);
struct DB *dbcreate (char *file, struct DBC *conf);

int db_close (struct DB *db);
int db_del (struct DB *, void *, size_t);
int db_get (struct DB *, void *, size_t, void **, size_t *);
int db_put (struct DB *, void *, size_t, void * , size_t  );

int db_sync (const struct DB *db);
