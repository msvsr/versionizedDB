#include"versionizedDB.h"

//Checks whether a file has correct value pairs or not. //(col=value)
int check_value_pair(char * value_pair){
	int len = strlen(value_pair), i;
	for (i = 0; i < len; i++) if (value_pair[i] == '=') break;
	if (i>0 && i < len - 1) return 1;
	return 0;
}

//Returns tokens in a given string as a structure of no of tokens and tokens.
tokens * get_tokens(char * query){
	char * token;
	tokens * Tkns;

	Tkns = (tokens*)malloc(sizeof(tokens));
	Tkns->type = -1;
	Tkns->no_of_tokens = 0;
	Tkns->tokens = NULL;

	token = strtok(query," ");
	while (token != NULL){
		Tkns->tokens = (char**)realloc(Tkns->tokens,sizeof(char*)*(++(Tkns->no_of_tokens)));
		Tkns->tokens[Tkns->no_of_tokens-1] = (char*)malloc(sizeof(char)*(strlen(token)+1));
		strcpy(Tkns->tokens[Tkns->no_of_tokens - 1],token);
		token = strtok(NULL," ");
	}
	return Tkns;
}

/*
[Returns structure{ type,tokens and no_of_tokens}] //Returns NULL for invalid queries.
--------------------
//no of tokens = 0
0)NULL query.
--------------------
//no of tokens = 2
1)show databases    // (shows all databases)
2)show database     //(prints currently working database)
3)show tables       //(prints tables names in a current database)
---------------------
//no of tokens = 3
4)create database database_name
5)create table table_name
--
6)drop database database_name
7)drop table table_name
--
8)truncate database database_name
9)truncate table table_name
--
10)use database database_name
---------------------
//no of tokens = 4
11)get id(integer) from table_name
12)del id(integer) from table_name
13)scan id1(integer):id2(integer) from table_name  //(if id2>id1 it will swap them)
---------------------
//no of tokens > 4
14)put into table_name id(integer) colname1=value1 [colname2=value2 colname3=value3 colname4=value4 . . .]

-----------------------------------------
//no of tokens = 1
15)commit
*/


tokens * process_query(char * query){
	int i = 0;

	tokens * Tkns;
	Tkns = get_tokens(query);

	switch (Tkns->no_of_tokens){
	case 0: { Tkns->type = 0; return Tkns; }
	case 1: if (strcmp(Tkns->tokens[0], "commit") == 0) { Tkns->type = 15; return Tkns; }
			else return NULL;

	case 2:if (strcmp(Tkns->tokens[0], "show") == 0){
		if (strcmp(Tkns->tokens[1], "databases") == 0){ Tkns->type = 1; return Tkns; }
		if (strcmp(Tkns->tokens[1], "database") == 0) { Tkns->type = 2; return Tkns; }
		if (strcmp(Tkns->tokens[1], "tables") == 0)   { Tkns->type = 3; return Tkns; }
		else return NULL;
	}
		   else return NULL;

	case 3:if (strcmp(Tkns->tokens[0], "create") == 0){
		if (strcmp(Tkns->tokens[1], "database") == 0) { Tkns->type = 4; return Tkns; }
		if (strcmp(Tkns->tokens[1], "table") == 0) { Tkns->type = 5; return Tkns; }
		else return NULL;
	}
		   else if (strcmp(Tkns->tokens[0], "drop") == 0){
			   if (strcmp(Tkns->tokens[1], "database") == 0) { Tkns->type = 6; return Tkns; }
			   if (strcmp(Tkns->tokens[1], "table") == 0) { Tkns->type = 7; return Tkns; }
			   else return NULL;
		   }
		   else if (strcmp(Tkns->tokens[0], "truncate") == 0){
			   if (strcmp(Tkns->tokens[1], "database") == 0) { Tkns->type = 8; return Tkns; }
			   if (strcmp(Tkns->tokens[1], "table") == 0) { Tkns->type = 9; return Tkns; }
			   else return NULL;
		   }
		   else if (strcmp(Tkns->tokens[0], "use") == 0){
			   if (strcmp(Tkns->tokens[1], "database") == 0) { Tkns->type = 10; return Tkns; }
			   else return NULL;
		   }
		   else return NULL;
	case 4: if (strcmp(Tkns->tokens[2], "from") == 0){
		if (strcmp(Tkns->tokens[0], "get") == 0) { Tkns->type = 11; return Tkns; }
		if (strcmp(Tkns->tokens[0], "del") == 0) { Tkns->type = 12; return Tkns; }
		if (strcmp(Tkns->tokens[0], "scan") == 0) { Tkns->type = 13; return Tkns; }
		else return NULL;
	}
	default: if (strcmp(Tkns->tokens[0], "put") == 0 && strcmp(Tkns->tokens[1], "into") == 0){
		for (i = 4; i < Tkns->no_of_tokens; i++) if (!check_value_pair(Tkns->tokens[i])) return NULL;
		Tkns->type = 14;
		return Tkns;
	}
			 else return NULL;
	}
}


/*
create database db
create table tb
put into tb 1 name=satya age=22
put into tb 2 name=adi age=23 gen=male
put into tb 3 name=teja mailid=msv@gmail.com
create table tb1
put into tb1 1 name=sat age=22
put into tb1 2 name=adi age=23 gen=male
put into tb1 3 name=tej mailid=msv@gmail.com

*/