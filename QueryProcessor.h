#include"versionizedDB.h"

dbase * use_db(char * dir_name){
	char * name;
	name = (char *)malloc(sizeof(char*)*(7+strlen(dir_name)));
	strcpy(name,"mkdir ");
	strcat(name,dir_name);

	if (system(name) == 0){ 
		printf("\n The database you specified does not exists.....\n"); 
		strcpy(name, "rmdir ");
		strcat(name, dir_name);
		system(name);
		return NULL;
	}
	else {
		return create_database(dir_name); 
	}
}


//
tab * get_table_by_name(dbase * db,char * name){

	int i = 0;
	for (i = 0; i < db->no_of_tables; i++){
		if (strcmp(db->tables[i]->table_name, name) == 0){
			return  db->tables[i];
		}
	}
	return NULL;
}

int check_value_pair(char * value_pair){
	int len = strlen(value_pair),i;
	for (i = 0; i < len; i++) if (value_pair[i] == '=') break;
	if (i>0 && i < len - 1) return 1;
	return 0;
}
//
void processquery(char * query,dbase *db){
	char * component, **col_values = NULL;
	tab * table;
	int key, pairs = 0, key1, key2;
	
	component = strtok(query, " ");
	
	
    
	if (db == NULL) {
		printf("\nSelect/Create a database.....\n");
		return;
	}
		//put into table_name id name=satya age=21
		if (strcmp(component, "put") == 0){
			component = strtok(NULL, " ");
			if (strcmp(component, "into") == 0){
				component = strtok(NULL, " ");
				table = get_table_by_name(db, component);
				if (table == NULL) { printf("\nTable does not exists.....\n"); return; }
				key = atoi(strtok(NULL, " "));

				component = strtok(NULL, " ");
				if (component != NULL){
					while (component != NULL){
						if (check_value_pair(component)){
							col_values = (char **)realloc(col_values, sizeof(char*)*(++pairs));
							col_values[pairs - 1] = (char*)malloc(sizeof(char*)*(strlen(component) + 1));
							strcpy(col_values[pairs - 1], component);
						}
						else{
							printf("\nSpecify the column and values correctly..\n");
							return;
						}
						component = strtok(NULL," ");
					}
					put(table, key, col_values, pairs);
				}
				else{
					printf("\nNo cols and values are given.....\n");
					return;
				}
			}
		}
		//get id from table_name
		else if (strcmp(component, "get") == 0){
			key = atoi(strtok(NULL, " "));
			if (strcmp(strtok(NULL, " "), "from") == 0){
				component = strtok(NULL, " ");
				table = get_table_by_name(db, component);
				if (strtok(NULL, " ") != NULL) { printf("Enter a valid query....."); return; }
				if (table == NULL) { printf("\nTable does not exists.....\n"); return; }
				printer(get(table, key));
			}
		}
		//del id from table_name
		else if (strcmp(component, "del") == 0){
			key = key = atoi(strtok(NULL, " "));
			if (strcmp(strtok(NULL, " "), "from") == 0){
				component = strtok(NULL, " ");
				table = get_table_by_name(db, component);
				if (strtok(NULL, " ") != NULL) { printf("Enter a valid query....."); return; }
				if (table == NULL) { printf("\nTable does not exists.....\n"); return; }
				delete(table, key);
			}
		}
		//scan id1:id2 from table_name
		else if (strcmp(component, "scan") == 0){
			component = strtok(NULL, " ");
			if (strcmp(strtok(NULL, " "), "from") == 0){
				table = get_table_by_name(db, strtok(NULL, " "));
				if (strtok(NULL, " ") != NULL) { printf("Enter a valid query....."); return; }
				if (table == NULL) { printf("\nTable does not exists.....\n"); return; }
				if (strtok(NULL, " ") == NULL){
					key1 = atoi(strtok(component, ":"));
					key2 = atoi(strtok(NULL, ":"));

					if (key1 > key2){
						key = key1;
						key1 = key2;
						key2 = key;
					}
					scan(table, key1, key2);
			}
		}
	}
}
