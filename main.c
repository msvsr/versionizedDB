///*
#define _CRT_SECURE_NO_WARNINGS
#include"process_query.h"

int main(){
	dbase *db;
	char query[256];
	tokens * tok;
	int key1, key2;
	db = NULL;

	printf("\n>>> ");
	scanf("%[^\n]", query);
	while (strcmp(query, "quit") != 0){
		tok = process_query(query);
		if (tok == NULL) printf("\n		Enter a valid query.....\n");
		else
		switch (tok->type){
		case 1:show_databases(); break;
		case 2:show_database(db); break;
		case 3:show_tables(db); break;

		case 4:db = create_db(tok->tokens[2],db); break;
		case 5:create_table(db,tok->tokens[2]); break;

		case 6:drop_database(db, tok->tokens[2]); break;
		case 7:drop_table(db, tok->tokens[2]); break;

		case 8:truncate_database(db, tok->tokens[2]); break;
		case 9:truncate_table(db, tok->tokens[2]); break;

		case 10:db = use_database(db, tok->tokens[2]); break;

		case 11:printer(get(get_table_by_name(db, tok->tokens[3]), atoi(tok->tokens[1]))); break;
		case 12:del(get_table_by_name(db, tok->tokens[3]), atoi(tok->tokens[1])); break;
		case 13:
			key1 = atoi(strtok(tok->tokens[1], ":"));
			key2 = atoi(strtok(NULL, ":"));
			scan(get_table_by_name(db, tok->tokens[3]),key1 ,key2 ); break;

		case 14:put(get_table_by_name(db, tok->tokens[2]), atoi(tok->tokens[3]), put_tokens(tok), tok->no_of_tokens - 4); break;

		case 15:flush(db); break;

		default:break;
		}
		fflush(stdin);
		printf("\n>>> ");
		scanf("%[^\n]", query);
	}

	return 0;
}
//*/
