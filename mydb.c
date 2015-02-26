
#include "mydb.h"
/*
int db_close(struct DB *db) {
//	return db->close(db);
}

int db_del(struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->deletet(db, &keyt);

}

int db_get(struct DB *db, void *key, size_t key_len,
	   void **val, size_t *val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->select(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}

int db_put(struct DB *db, void *key, size_t key_len,
	   void *val, size_t val_len) {

	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->insert(db, &keyt, &valt);
}
*/