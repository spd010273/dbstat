/*
 * DBStat - Written Jan 2018 by Chris Autry
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libpq-fe.h>
#include <math.h>
#include <string.h>

#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>

char * parse_args( int, char ** );
void _handle_modification( PGconn *, char *, char * );

int DEBUG = 0;

char * parse_args( int argc, char ** argv )
{
    /* Parse command line arguments */
    char * conninfo = NULL;
    char * username = NULL;
    char * dbname   = NULL;
    char * port     = NULL;
    char * hostname = NULL;

    int malloc_size = 0;
    int c;

    opterr = 0;

    while( ( c = getopt( argc, argv, "U:p:d:h:D" ) ) != -1 )
    {
        switch( c )
        {
            case 'U':
                username = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'd':
                dbname = optarg;
                break;
            case 'h':
                hostname = optarg;
                break;
            case 'D':
                DEBUG = 1;
                break;
            case '?':
                break;
            default:
                printf( "Invalid argument: %c\n", (char) c );
                exit( 1 );
        }
    }

    if( port == NULL )
    {
        port = "5432";
    }

    if( username == NULL )
    {
        username = "postgres";
    }

    if( hostname == NULL )
    {
        hostname = "localhost";
    }

    if( dbname == NULL )
    {
        dbname = username;
    }

    /*
        malloc connection string
        accounting for
        'username=? host=? port=? dbname=?'
    */
    malloc_size = strlen( username )
                + strlen( port )
                + strlen( dbname )
                + strlen( hostname )
                + 26;

    /* add 26 bytes to account for const strings + null terminator */
    conninfo = ( char * ) malloc( sizeof( char ) * ( malloc_size ) );

    /* Assemble the connections string */
    strcpy( conninfo, "user=" );
    strcat( conninfo, username );
    strcat( conninfo, " host=" );
    strcat( conninfo, hostname );
    strcat( conninfo, " port=" );
    strcat( conninfo, port );
    strcat( conninfo, " dbname=" );
    strcat( conninfo, dbname );

    return conninfo;
}

void _handle_modification( PGconn * conn, char * channel, char * operation )
{
    char * command;
    int malloc_size = 0;
    PGresult * result;
    char * row_delta;
    char * zero = "0"; 
    char * positive_one = "1";
    char * negative_one = "-1";
    
    row_delta = zero;
    /*
     *  INSERT INTO dbstat.tb_catalog_table_modification( oid, op, recorded ) 70
     *       SELECT c.oid, ?::CHAR, clock_timestamp() 40 (doesnt include '' around ?
     *         FROM pg_class c 16
     *         JOIN pg_namespace n 20
     *           ON n.oid = c.relnamespace 26
     *         JOIN dbstat.tb_catalog_table ct 32
     *           ON ct.oid = c.oid 18
     *        WHERE ct.schema_name || '.' || ct.table_name = ? 47 (not including '' around ? )
     */
    /* We'll need characters + variables malloced */
    malloc_size = strlen( channel )
                + strlen( operation )
                + 273;
    
    command = ( char * ) malloc( sizeof( char ) * malloc_size );
    
    if( command == NULL )
    {
        fprintf(
            stderr,
            "Malloc for stat logging failed!\n"
        );
        return;
    }

    strcpy( command, "INSERT INTO dbstat.tb_catalog_table_modification( oid, op, recorded ) " );
    strcat( command, "SELECT c.oid, '" );
    strcat( command, operation );
    strcat( command, "'::CHAR, clock_timestamp() " );
    strcat( command, "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " );
    strcat( command, "JOIN dbstat.tb_catalog_table ct ON ct.oid = c.oid " );
    strcat( command, "WHERE ct.schema_name || '.' || ct.table_name = '" );
    strcat( command, channel );
    strcat( command, "'\0" );

    result = PQexec( conn, command );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        fprintf(
            stderr,
            "INSERT on stats table failed: %s\n",
            PQerrorMessage( conn )
        );

        PQclear( result );
        PQfinish( conn );
        free( command );
        return;
    }
    
    PQclear( result );
    free( command );
    malloc_size = 0;

    if( strcmp( operation, "DELETE" ) == 0 )
    {
        row_delta = negative_one;
    }
    else if( strcmp( operation, "INSERT" ) == 0 )
    {
        row_delta = positive_one;
    }

    /* UPDATE dbstat.tb_catalog_table SET row_count = row_count + ? WHERE schema_name || '.' || table_name = '?' */
    /* need to malloc 97 chars */
    malloc_size = strlen( channel )
                + strlen( row_delta )
                + 104;
    
    command = ( char * ) malloc( sizeof( char ) * malloc_size );
    
    if( command == NULL )
    {
        fprintf(
            stderr,
            "Failed to malloc row count update command!\n"
        );
    }

    strcpy( command, "UPDATE dbstat.tb_catalog_table SET row_count = row_count + " );
    strcat( command, row_delta );
    strcat( command, " WHERE schema_name || '.' || table_name = '" );
    strcat( command, channel );
    strcat( command, "'\0" );

    result = PQexec( conn, command );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        fprintf(
            stderr,
            "UPDATE of row_count failed: %s",
            PQerrorMessage( conn )
        );

        PQclear( result );
        PQfinish( conn );
        free( command );
        return;
    }

    free( command );
    return;
}

int main( int argc, char ** argv )
{
    char * conninfo;
    PGconn * conn;
    int notify_count;
    PGnotify * notify;
    PGresult * result;
    PGresult * listen_result;
    int row_count = 0;
    int i = 0;
    int malloc_size = 0;
    char * listen_command;
    char * schema_name;
    char * table_name;

    conninfo = parse_args( argc, argv );

    if( conninfo == NULL )
    {
        return 1;
    }

    /* Connect to the database */
    conn = PQconnectdb( conninfo );
    free( conninfo );

    if( PQstatus( conn ) != CONNECTION_OK )
    {
        fprintf(
            stderr,
            "Connection to database failed: %s\n",
            PQerrorMessage( conn )
        );

        PQfinish( conn );
        return 1;
    }

    if( DEBUG )
    {
        printf( "Connected to database\n" );
    }

    /* Enumerate the channels we should be listening on */
    result = PQexec(
        conn,
        "SELECT schema_name, table_name FROM dbstat.tb_catalog_table ORDER BY table_name"
    );

    if( PQresultStatus( result ) != PGRES_TUPLES_OK )
    {
        fprintf(
            stderr,
            "Failed to get catalog of tables: %s\n",
            PQerrorMessage( conn )
        );

        PQclear( result );
        PQfinish( conn );
        return 1;
    }

    row_count = PQntuples( result );

    for( i = 0; i < row_count; i++ )
    {
        schema_name = PQgetvalue( result, i, PQfnumber( result, "schema_name" ) );
        table_name  = PQgetvalue( result, i, PQfnumber( result, "table_name" ) );

        if( DEBUG )
        {
            fprintf(
                stdout,
                "Creating channel for: %s.%s\n",
                schema_name,
                table_name
            );
        }

        /* Command: LISTEN "?.?" */
        /* We'll have to malloc 10 extra bytes + 1 for null terminator */
        malloc_size = strlen( schema_name )
                    + strlen( table_name )
                    + 12;

        listen_command = ( char * ) malloc( sizeof( char ) * ( malloc_size ) );

        if( listen_command == NULL )
        {
            printf( "Malloc for channel failed\n" );
            return 1;
        }

        strcpy( listen_command, "LISTEN \"" );
        strcat( listen_command, schema_name );
        strcat( listen_command, "." );
        strcat( listen_command, table_name );
        strcat( listen_command, "\"\0" );

        listen_result = PQexec( conn, listen_command );

        if( PQresultStatus( listen_result ) != PGRES_COMMAND_OK )
        {
            fprintf(
                stderr,
                "Listen command %s failed: %s\n",
                listen_command,
                PQerrorMessage( conn )
            );

            PQclear( listen_result );
            PQclear( result );
            PQfinish( conn );
            free( listen_command );
            return 1;
        }

        PQclear( listen_result );
        free( listen_command );
        malloc_size = 0;
    }

    PQclear( result );

    notify_count = 0;
    notify       = NULL;

    while( 1 )
    {
        int sock;
        fd_set input_mask;
        sock = PQsocket( conn );

        if( sock < 0 )
        {
            break;
        }

        FD_ZERO( &input_mask );
        FD_SET( sock, &input_mask );

        if( select( sock + 1, &input_mask, NULL, NULL, NULL ) < 0 )
        {
            fprintf( stderr, "select() failed: %s\n", strerror( errno ) );
            PQfinish( conn );
            return 1;
        }

        PQconsumeInput( conn );

        while( ( notify = PQnotifies( conn ) ) != NULL )
        {
            char * relname;
            int backend_pid;
            char * operation;

            relname     = notify->relname;
            backend_pid = notify->be_pid;
            operation   = notify->extra;

            if( DEBUG )
            {
                fprintf(
                   stderr,
                   "ASYNCRONOUS NOTIFY of '%s' received from backend PID %d with payload '%s'\n",
                   relname,
                   backend_pid,
                   operation
                );
            }

            _handle_modification( conn, relname, operation );
            PQfreemem( notify );
            notify_count++;
        }
    }

    PQfinish( conn );
    return 0;
}
