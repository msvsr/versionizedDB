struct cell{
	int version;
	char * value;
	struct cell *pre_ver;
};

struct record_col{
	char *name;
	struct cell  * data;
	struct record_col *next;
};
struct record{
	int key;
	int commit;
	struct record_col *col;
};

struct table{
	char *table_name;
	int count_rows;
	struct record **rows;
};

struct database{
	char *dbname;
	int no_of_tables;
	struct table **tables;
};

struct g_record{
	int commit;
	struct g_row * row;
};
struct g_row{
	char * col_name;
	char * value;
	struct g_row * next;
};

struct Tokens{
	int type;
	int no_of_tokens;
	char ** tokens;
};

typedef struct database dbase;
typedef struct table tab;
typedef struct record rec;
typedef struct record_col re_col;
typedef struct cell data;
typedef struct g_row r_row;
typedef struct g_record r_rec;
typedef struct Tokens tokens;