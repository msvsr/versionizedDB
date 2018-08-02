//Implementing DB with versions.(NoSQL)

#define _CRT_SECURE_NO_WARNINGS
#include"DataStructures.h"
#include"stdio.h"
#include"stdlib.h"
#include"string.h"

//prototypes
int make_dir(char * db_name);
int remove_dir(char *db_name);
char * concat_path(char *folder_name, char * file, char * connector);
char * get_path(char * folder, char * tablename, char * colname);
void write_to_file(char * path, int  id, char * value);
void write_into_index_file(char * folder, char ** paths, int no_of_paths);
char ** save_paths(char * path, char ** paths, int * no_of_paths);
void flush(dbase * db);
void load_records(tab * tb, FILE * fp, char * col_name);
dbase * import_data(char * db_name);
void show_databases();
dbase * create_database(char *db_name);
void create_table(dbase * db, char * table_name);
tab * get_table_by_name(dbase * db, char * name);
data * get_new_cell(char * value, int version);
re_col * get_new_col(char *name);
void insert_into_row(rec * tuple, char ** values, int pairs);
void printer(rec * p_data);
void put(tab * ctab, int key, char ** values, int pairs);
void del(tab * ctab, int key);
rec * get(tab *ctab, int key);
void del(tab * ctab, int key);



//creates folder for database.
int make_dir(char * db_name){
	char * cmd;
	cmd = (char*)malloc(sizeof(char)*(strlen(db_name) + 7));
	strcpy(cmd, "mkdir ");
	strcat(cmd, db_name);
	if (system(cmd) == 0) return 1;
	else return 0;
}

//removes folder of database.
int remove_dir(char *db_name){
	char * cmd;
	cmd = (char*)malloc(sizeof(char)*(strlen(db_name) + 7));
	strcpy(cmd, "RMDIR /S ");
	strcat(cmd, db_name);
	if (system(cmd) == 0) return 1;
	else return 0;
}

//concat

char * concat_path(char *folder_name, char * file, char * connector){
	char * path;
	path = (char*)malloc(strlen(folder_name) + strlen(file) + strlen(connector) + 1);
	strcpy(path, folder_name);
	strcat(path, connector);
	strcat(path, file);
	return path;
}

char * get_path(char * folder, char * tablename, char * colname){
	return concat_path(folder, concat_path(tablename, concat_path(colname, "txt", "."), "@"), "/");
}

//write a cell into a file userid,value
void write_to_file(char * path, int  id, char * value){
	FILE *fp;
	fp = fopen(path, "a");
	fprintf(fp, "%d,%s\n", id, value);
	fclose(fp);
}

//writes into index file in a db. index.txt
void write_into_index_file(char * folder, char ** paths, int no_of_paths){
	FILE *fp;
	int i;
	fp = fopen(concat_path(folder, "index.txt", "/"), "a");
	for (i = 0; i < no_of_paths; i++) fprintf(fp, "%s\n", paths[i]);
	fclose(fp);
}

//saves paths set.
char** save_paths(char * path, char ** paths, int * no_of_paths){
	int i = 0;
	if (paths == NULL){
		paths = (char **)realloc(paths, sizeof(char*));
		paths[0] = (char*)malloc(sizeof(char)*(strlen(path) + 1));
		*no_of_paths = 1;
		strcpy(paths[0], path);
	}
	else{
		for (i = 0; i < *no_of_paths; i++) if (strcmp(path, paths[i]) == 0) break;
		if (i == *no_of_paths){
			paths = (char **)realloc(paths, sizeof(char*)*(++(*no_of_paths)));
			paths[i] = (char*)malloc(sizeof(char)*(strlen(path) + 1));
			strcpy(paths[*no_of_paths - 1], path);
		}
	}
	return paths;
}
//writes all the data in db to different files in to db folder.
void flush(dbase * db){
	int i = 0, j = 0, no_of_paths = 0;
	re_col * temprec;
	char *path;
	char ** table_col_paths = NULL;
	if (db == NULL){ printf("\nThere is no database in use to commit.....\n"); return; }
	if (!make_dir(db->dbname)) { remove_dir(db->dbname); make_dir(db->dbname); }

	for (i = 0; i < db->no_of_tables; i++){
		for (j = 0; j < db->tables[i]->count_rows; j++){
			temprec = db->tables[i]->rows[j]->col;
			while (temprec != NULL){
				path = get_path(db->dbname, db->tables[i]->table_name, temprec->name);
				write_to_file(path, db->tables[i]->rows[j]->key, temprec->data->value);
				temprec = temprec->next;

			table_col_paths=save_paths(path, table_col_paths, &no_of_paths);
			}
		}

		write_into_index_file(db->dbname, table_col_paths, no_of_paths);
		free(table_col_paths);
		table_col_paths = NULL;
		no_of_paths=0;
	}
}

//loads records into table col by col.
void load_records(tab * tb, FILE * fp, char * col_name){
	char record[100];
	int key;
	char * data;
	char ** values;
	values = (char **)malloc(sizeof(char *));
	while (fscanf(fp, "%s", record) > 0){
		key = atoi(strtok(record, ","));
		data = strtok(NULL, ",");

		values[0] = concat_path(col_name, data, "=");
		put(tb, key, values, 1);
	}
}

//create all tables from index.txt
void create_tables(dbase * db, FILE *fp){
	int no_of_tables=0;
	char path[100], *tablename;
	char ** tablenames;
	int i = 0;
	tablenames = NULL;
	if (fp != NULL){
		while (fscanf(fp, "%s", path) > 0){
			if (tablenames == NULL){
				tablenames = (char**)realloc(tablenames,sizeof(char*));
				strtok(path, "/");
				tablename = strtok(NULL, "@");
				tablenames[0]=(char*)malloc(sizeof(char)*(strlen(tablename) + 1));
				no_of_tables = 1;
				strcpy(tablenames[0], tablename);
			}
			else{
				strtok(path, "/");
				tablename = strtok(NULL, "@");
				for (i = 0; i < no_of_tables; i++) if (strcmp(tablename, tablenames[i]) == 0) break;
				if (i == no_of_tables){
					tablenames = (char**)realloc(tablenames, sizeof(char*)*(no_of_tables + 1));
					tablenames[no_of_tables] = (char*)malloc(sizeof(char)*(strlen(tablename) + 1));
					no_of_tables++;
					strcpy(tablenames[no_of_tables - 1], tablename);
				}
			}
			
		}

		db->tables = NULL;
		db->no_of_tables = 0;
		while (no_of_tables--){
			create_table(db,tablenames[no_of_tables]);
		}
	}
	else printf("\nDatabase does not exits......\n");
}

//imports database db_name from folder into dbase pointer.
dbase * import_data(char * db_name){
	dbase * db;
	FILE *fp, *table_col_fp;
	char * old_table_name;
	char path[100], *tablename, *col_name;

	db = (dbase *)malloc(sizeof(dbase));


	old_table_name = NULL;

	fp = fopen(concat_path(db_name, "index.txt", "/"), "r");
	if (fp != NULL){
		db = create_database(db_name);
		create_tables(db,fp);
		rewind(fp);
		while (fscanf(fp, "%s", path) > 0){
			
			table_col_fp = fopen(path, "r");
			strtok(path,"/");
			tablename=strtok(NULL,"@");
			col_name = strtok(NULL,".");

			load_records(get_table_by_name(db, tablename), table_col_fp, col_name);

			fclose(table_col_fp);
		}
		fclose(fp);
		return db;
	}
	else printf("\n     No database exists.....\n");
	return NULL;
}



void show_databases(){
	printf("Will display soon.....");
}

//Creating new database with given name.
dbase * create_database(char *db_name){
	//
	dbase * new_database;
	//
	new_database = (dbase*)malloc(sizeof(dbase));
	new_database->dbname = (char*)malloc(sizeof(char)*(strlen(db_name)+1));
	new_database->no_of_tables = 0;
	new_database->tables = NULL;
	//
	strcpy(new_database->dbname, db_name);
	//
	return new_database;

}


//Creating a table in a database.
void create_table(dbase * db,char * table_name){
	int i = 0;
	if (db == NULL) { printf("\n select/create a database.....\n "); return; }
	for (i = 0; i < db->no_of_tables; i++) 
		if (strcmp(db->tables[i]->table_name, table_name) == 0){
			printf("\nTable already exits....\n");
			return;
		}
	//
	db->tables = (tab **)realloc(db->tables,sizeof(tab*)*(++(db->no_of_tables)));
	db->tables[db->no_of_tables - 1] = (struct table *)malloc(sizeof(struct table));
	db->tables[db->no_of_tables - 1]->table_name = (char*)malloc(sizeof(char)*(strlen(table_name)+1));
	db->tables[db->no_of_tables - 1]->rows = NULL;
	db->tables[db->no_of_tables - 1]->count_rows = 0;
	//
	strcpy(db->tables[db->no_of_tables - 1]->table_name,table_name);
}

tab * get_table_by_name(dbase * db, char * name){

	int i = 0;

	if (db != NULL)
	for (i = 0; i < db->no_of_tables; i++){
		if (strcmp(db->tables[i]->table_name, name) == 0){
			return  db->tables[i];
		}
	}
	printf("\nselect/create a database.....\n");
	return NULL;
}


data * get_new_cell(char * value,int version){
	data * new_cell;
	
	new_cell = (data *)malloc(sizeof(data));
	new_cell->value = (char*)malloc(sizeof(char)*(strlen(value)+1));
	new_cell->version = version;
	new_cell->pre_ver = NULL;
	
	strcpy(new_cell->value,value);

	return new_cell;
}

re_col * get_new_col(char *name){
	
	re_col * new_col;
	
	new_col = (re_col*)malloc(sizeof(re_col));
	new_col->name = (char*)malloc(sizeof(char)*(strlen(name)+1));
	new_col->next = NULL;
	new_col->data = NULL;

	strcpy(new_col->name,name);

	return new_col;
}


void insert_into_row(rec * tuple, char ** values, int pairs){
	re_col * temp_col;
	char *col_name, *col_value;
	data * temp_cell;
	if (tuple->col == NULL){
		while (pairs--){
			col_name = strtok(values[pairs], "=");
			col_value = strtok(NULL, "=");
			if (tuple->col == NULL){
				tuple->col = get_new_col(col_name);
				tuple->col->data = get_new_cell(col_value, tuple->commit+1);
				tuple->col->next = NULL;
			}
			else{
				temp_col = tuple->col;
				while (temp_col->next != NULL) temp_col = temp_col->next;
				temp_col->next = get_new_col(col_name);
				temp_col->next->data = get_new_cell(col_value, tuple->commit+1);
				temp_col->next->next = NULL;
			}
		}
	}
	else{
		while (pairs--){
			col_name = strtok(values[pairs], "=");
			col_value = strtok(NULL, "=");
			
			temp_col = tuple->col;
			while (temp_col->next != NULL){
				if (strcmp(temp_col->name,col_name) == 0){
					if (tuple->commit == temp_col->data->version){
						temp_cell = get_new_cell(col_value, tuple->commit + 1);;
						temp_cell->pre_ver = temp_col->data;
						temp_col->data = temp_cell;
					}
					break;
				}
				temp_col = temp_col->next;
			}
			if (temp_col->next == NULL){
				if (strcmp(temp_col->name,col_name) == 0){
					if (tuple->commit == temp_col->data->version){
						temp_cell = get_new_cell(col_value, tuple->commit + 1);;
						temp_cell->pre_ver = temp_col->data;
						temp_col->data = temp_cell;
					}
				}
				else{
					temp_col->next=get_new_col(col_name);
					temp_col->next->data = get_new_cell(col_value, tuple->commit+1);
					temp_col->next->next = NULL;
				}
			}
		}
	}
}

//
void printer(rec * p_data){
	re_col * col;
	data *d;

	if (p_data == NULL) { printf("\nThere is no record with given key...."); return; }
	col = p_data->col;
	printf("\n\t%d:{\n", p_data->commit);
	while (col != NULL){
		d = col->data;
		while (d != NULL){
			if (d->version <= p_data->commit)
			{
				printf("%s=%s---%d  ", col->name, d->value, d->version);
				break;
			}

			d = d->pre_ver;
		}
		printf("\n");
		col = col->next;
	}
	printf("\t}");
}


//Putting data into table.
void put(tab * ctab, int key,char ** values, int pairs){
	//row * temp_row;
	int i = 0;
	if (ctab == NULL) return;
	if (ctab->rows == NULL){

		ctab->rows = (rec**)realloc(ctab->rows,sizeof(rec*)*(++(ctab->count_rows)));
		ctab->rows[ctab->count_rows - 1] = (rec*)malloc(sizeof(rec));
		ctab->rows[ctab->count_rows - 1]->key = key;
		ctab->rows[ctab->count_rows - 1]->commit = 0;
		ctab->rows[ctab->count_rows - 1]->col = NULL;

		insert_into_row(ctab->rows[0], values, pairs);
		ctab->rows[0]->commit++;
	}
	else{
		for (i = 0; i < ctab->count_rows; i++){
			if (ctab->rows[i]->key == key){
				insert_into_row(ctab->rows[i], values, pairs);
				ctab->rows[i]->commit++;
				break;
			}
		}
		
		if (i==ctab->count_rows){
			ctab->rows = (rec**)realloc(ctab->rows, sizeof(rec*)*(++(ctab->count_rows)));
			ctab->rows[ctab->count_rows - 1] = (rec*)malloc(sizeof(rec));
			ctab->rows[ctab->count_rows - 1]->key = key;
			ctab->rows[ctab->count_rows - 1]->commit = 0;
			ctab->rows[ctab->count_rows - 1]->col = NULL; 
			insert_into_row(ctab->rows[ctab->count_rows - 1], values, pairs);
			ctab->rows[ctab->count_rows - 1]->commit++;
		}
	}
}


//
rec * get(tab *ctab,int key){
	if (ctab == NULL) return NULL;
	if (ctab->rows!=NULL)
	for (int i = 0; i < ctab->count_rows; i++){
		if (ctab->rows[i]->key == key && ctab->rows[i]->commit != 0){
			return	ctab->rows[i];
		}
	}
	return NULL;
}

//
void del(tab * ctab,int key){
	if (ctab == NULL) return;
	for (int i = 0; i < ctab->count_rows; i++){
		if (ctab->rows[i]->key == key){
			ctab->rows[i] = ctab->rows[ctab->count_rows-1];
			ctab->count_rows--;
			if (ctab->count_rows == 0) ctab->rows = NULL;
			else ctab->rows = (rec**)realloc(ctab->rows,sizeof(rec*)*(ctab->count_rows));
			return;
		}
	}
	printf("\nThere is no record with the given key....");
}

//
void scan(tab * ctab,int low, int high){
	int i = 0;
	if (ctab == NULL) return;
	for (i = 0; i < ctab->count_rows; i++){
		if (ctab->rows[i]->key >= low&&ctab->rows[i]->key <= high){
			printf("%d:--", ctab->rows[i]->key);
			printer(ctab->rows[i]);
			printf("\n");
		}
	}
}


void show_database(dbase *db){
	if (db != NULL)
		printf("%s\n", db->dbname);
	else
		printf("\nNo Database selected\n");
}

void show_tables(dbase *db){
	int i = 0;
	if (db != NULL){
		if (db->tables==NULL) printf("\nNo tables in the database.....\n");
		else
		for (i = 0; i < db->no_of_tables; i++){
			printf("%s\n", db->tables[i]->table_name);
		}
	}
	else
		printf("\nNo Database selected\n");
}

dbase * create_db(char * dbname,dbase *db){
	char ch;
	if (db == NULL){
		return create_database(dbname);
	}
	else{
		printf("\n Do you want to commit existing one.... [%s].  (y/n): ", db->dbname);
		fflush(stdin);
		scanf("%c", &ch);
		if (ch == 'y'){
			flush(db);
			return create_database(dbname);
	     }
		else return db;
		}
}

void drop_database(dbase * db, char * dbname){
	if (db!=NULL)
	if (strcmp(db->dbname, dbname) == 0) { free(db), db = NULL; }
	if (remove_dir(dbname) == 0) printf("\nDatabase [%s] does not exits....\n",dbname);
}

void drop_table(dbase * db, char * name){
	int i = 0;
	for (i = 0; i < db->no_of_tables; i++){
		if (strcmp(db->tables[i]->table_name, name) == 0) {
			db->tables[i] = db->tables[db->no_of_tables-1];
			db->tables = (tab **)realloc(db->tables,sizeof(tab*)*(--(db->no_of_tables)));
		}
	}
}

void truncate_database(dbase *db,char * dbname){
	if (db!=NULL)
	if (strcmp(db->dbname, dbname) == 0) { free(db), db = NULL; }
	if (remove_dir(dbname) == 0) printf("\nDatabase [%s] does not exits....\n", dbname);
	else make_dir(dbname);
}

void truncate_table(dbase *db,char * table_name){
	if (db == NULL) { printf("\nNo Database selected.....\n"); return; }
	tab * table = get_table_by_name(db,table_name);
	free(table->rows);
	table->count_rows = 0;
	table->rows = NULL;
}

dbase * use_database(dbase *db,char * dbname){
	char ch;
	if (db == NULL){
		return import_data(dbname);
	}
	else{
		printf("\n Do you want to commit existing one.... [%s].  (y/n): ", db->dbname);
		fflush(stdin);
		scanf("%c", &ch);
		if (ch == 'y') { flush(db); return import_data(dbname); }
		else return db;
	}

}

char ** put_tokens(tokens * tok){
	char ** values;
	int i;
	values = (char**)malloc(sizeof(char*)*(tok->no_of_tokens-4));

	for (i = 4; i < tok->no_of_tokens; i++){
		values[i - 4] = tok->tokens[i];
	}
	return values;
}