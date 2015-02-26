#include <stddef.h>

/* check `man dbopen` */
struct DBT {
	void  *data;
	size_t size;
};

struct DB {
	/* Public API */
	/* Returns 0 on OK, -1 on Error */
	int (*close)(struct DB *db);
	int (*deletet)(struct DB *db, struct DBT *key);
	int (*insert)(struct DB *db, struct DBT *key, struct DBT *data);
	/* * * * * * * * * * * * * *
	 * Returns malloc'ed data into 'struct DBT *data'.
	 * Caller must free data->data. 'struct DBT *data' must be alloced in
	 * caller.
	 * * * * * * * * * * * * * */
	int (*select)(struct DB *db, struct DBT *key, struct DBT *data);
	/* Sync cached pages with disk
	 * */
	int (*sync)(struct DB *db);
	/* For future uses - sync cached pages with disk
	 * int (*sync)(const struct DB *db)
	 * */
	/* Private API */
	/*     ...     */
}; /* Need for supporting multiple backends (HASH/BTREE) */

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

/* Open DB if it exists, otherwise create DB */
struct DB *dbopen(char *file, struct DBC *conf);

int db_close(struct DB *db);
int db_delete(struct DB *, void *, size_t);
int db_select(struct DB *, void *, size_t, void **, size_t *);
int db_insert(struct DB *, void *, size_t, void * , size_t  );

/* Sync cached pages with disk */
int db_sync(const struct DB *db);
