#include<stdlib.h>
#include"string.h"

//creates folder for database.
int make_dir(char * db_name){
	char * cmd;
	cmd = (char*)malloc(sizeof(char)*(strlen(db_name)+7));
	strcpy(cmd,"mkdir ");
	strcat(cmd,db_name);
	if (system(cmd) == 0) return 1;
	else return 0;
}

//removes folder of database.
int remove_dir(char *db_name){
	char * cmd;
	cmd = (char*)malloc(sizeof(char)*(strlen(db_name) + 7));
	strcpy(cmd, "rm -r ");
	strcat(cmd, db_name);
	if (system(cmd) == 0) return 1;
	else return 0;
}

//concat

char * concat_path(char *folder_name,char * file,char * connector){
	char * path;
	path = (char*)malloc(strlen(folder_name)+strlen(file)+strlen(connector)+1);
	strcpy(path,folder_name);
	strcat(path,connector);
	strcat(path,file);
	return path;
}

char * get_path(char * folder,char * tablename,char * colname){
	return concat_path(folder, concat_path(tablename, concat_path(colname,"txt","."), "@"), "/");
}

//write a cell into a file userid,value
void write_to_file(char * path,int  id,char * value){
	FILE *fp;
	fp = fopen(path,"a");
	fprintf(fp,"%d,%s\n",id,value);
	fclose(fp);
}

//writes into index file in a db. index.txt
void write_into_index_file(char * folder,char ** paths, int no_of_paths){
	FILE *fp;
	int i;
	fp = fopen(concat_path(folder, "index.txt", "/"),"a");
	for (i = 0; i < no_of_paths; i++) fprintf(fp,"%s\n",paths[i]);
	fclose(fp);
}

//saves paths set.
void save_paths(char * path, char ** paths,int * no_of_paths){
	int i = 0;
	if (paths == NULL){
		paths = (char **)realloc(paths, sizeof(char*));
		paths[0] = (char*)malloc(sizeof(char)*(strlen(path)+1));
		strcpy(paths[0],path);
	}
	else{
		for (i = 0; i < *no_of_paths; i++) if (strcmp(path, paths[i]) == 0) break;
		if (i == *no_of_paths){
			paths = (char **)realloc(paths, sizeof(char*)*(++(*no_of_paths)));
			paths[i] = (char*)malloc(sizeof(char)*(strlen(path) + 1));
			strcpy(paths[*no_of_paths-1], path);
		}
	}
}
//writes all the data in db to different files in to db folder.
void flush(dbase * db){
	int i = 0, j = 0,no_of_paths=0;
	record_col * temprec;
	char *path;
	char ** table_col_paths=NULL;
	if (!make_dir(db->dbname)) { remove_dir(db->dbname); make_dir(db->dbname); }

	for (i = 0; i < db->no_of_tables; i++){
		for (j = 0; j < db->tables[i]->count_rows; j++){
			temprec = db->tables[i]->rows[j]->col;
			while (temprec != NULL){
				path=get_path(db->dbname, db->tables[i]->table_name,temprec->name);
				write_to_file(path, db->tables[i]->rows[j]->key,temprec->data->value);
				temprec = temprec->next;

				save_paths(path,table_col_paths,&no_of_paths);
			}
		}

		write_into_index_file(db->dbname,table_col_paths,no_of_paths);
		free(table_col_paths);
		table_col_paths = NULL;
		no_of_paths;
	}
}

//loads records into table col by col.
void load_records(tab * tb,FILE * fp,char * col_name){
	char record[100];
	int key;
	char * data;
	while (fscanf(fp, "%s", record) > 0){
		key = atoi(strtok(record,","));
		data = strtok(NULL,",");
		put(tb, key, (char **)concat_path(col_name,data,"="),1);
	}
}
//imports database db_name from folder into dbase pointer.
dbase * import_data(char * db_name){
	dbase * db;
	tab * tb;
	FILE *fp,*table_col_fp;
	char * old_table_name;
	char path[100], *tablename, *col_name;

	old_table_name = NULL;

	fp = fopen(concat_path(db_name, "index.txt", "/"), "r");
	if (fp != NULL){
		db = create_database(db_name);
		while (fscanf(fp, "%s", path) > 0){
			table_col_fp = fopen(path, "r");

			strtok(path, "/");
			tablename = strtok(NULL, "@");
			col_name = strtok(NULL, ".");

			if (strcmp(tablename, old_table_name) != 0){
				create_table(db,tablename);
			}
			
			tb = get_table_by_name(db,tablename);
			load_records(tb,table_col_fp,col_name);

			fclose(table_col_fp);

			old_table_name = tablename;
		}
		fclose(fp);
	}
	else printf("\n     No database exists.....\n");
}


void show_databases(){
	printf("Will display soon.....");
}