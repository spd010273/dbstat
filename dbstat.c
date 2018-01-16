/*
 * DBStat - Written Jan 2018 by Chris Autry
 *
 * TODO:
 *  - Enable dbstat to connect to a foreign database for logging table statistics
 *  - Enable more detailed logging, like row diffs for UPDATES and PKs for INS/DEL
 *  - Enable multithreading for listen channels?
 *  - Clean up main() by pulling code into functions
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
#include <time.h>

/* Function prototypes */
void parse_args( int, char ** );
void _handle_modification( char *, char * );
PGresult * _execute_query( char *, char **, int );
void usage( char * );

/* Program globals */
PGconn * conn;
char * conninfo = NULL;
int DEBUG = 0;

static const char log_modification[] =
    "INSERT INTO dbstat.tb_catalog_table_modification "
    "            ( "
    "                oid, "
    "                op, "
    "                recorded "
    "            ) "
    "     SELECT c.oid, "
    "            $1::CHAR, "
    "            clock_timestamp() "
    "       FROM pg_class c "
    "       JOIN pg_namespace n "
    "         ON n.oid = c.relnamespace "
    "       JOIN dbstat.tb_catalog_table ct "
    "         ON ct.oid = c.oid "
    "      WHERE ct.schema_name || '.' || ct.table_name = $2";
static const char update_stats[] =
    " UPDATE dbstat.tb_catalog_table "
    "    SET row_count = row_count + $1 "
    "  WHERE schema_name || '.' || table_name = $2 ";

/* Functions */
void usage( char * message )
{
    if( message != NULL )
    {
        printf( "%s\n", message );
    }

    printf(
        "Usage: dbstat\n"
        "    -U DB user (default: postgres)\n"
        "    -p DB port (default: 5432)\n"
        "    -h DB host (default: localhost)\n"
        "    -d DB name (default: -U param )\n"
        "    -D DEBUG\n"
        "    -v VERSION\n"
        "    -? HELP\n"
   );

   exit( 1 );
}

void parse_args( int argc, char ** argv )
{
    /* Parse command line arguments */
    char * username = NULL;
    char * dbname   = NULL;
    char * port     = NULL;
    char * hostname = NULL;

    int malloc_size = 0;
    int c;

    opterr = 0;

    while( ( c = getopt( argc, argv, "U:p:d:h:Dv" ) ) != -1 )
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
                usage( NULL );
            case 'v':
                printf( "DB Statistics Collector. Version 0.1\n" );
                exit( 0 );
            default:
                usage( "Invalid argument:" );
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
        'username=? host=? port=? dbname=?\0'
        The ?'s are variables, and the size is accounted for
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

    if( DEBUG )
    {
        fprintf(
            stdout,
            "Parsed args: %s\n",
            conninfo
        );
    }

    return;
}

/*
 *  _execute_query( query, params )
 *
 *  Validates a connection, executes a query, and returns the result
 */
PGresult * _execute_query( char * query, char ** params, int param_count )
{
    PGresult * result;
    int retry_counter = 0;
    int last_backoff_time = 0;
    char * last_sql_state = NULL;

    while( PQstatus( conn ) != CONNECTION_OK && retry_counter < 3 )
    {
        fprintf(
            stderr,
            "Failed to connect to DB server (%s). Retrying...\n",
            PQerrorMessage( conn )
        );

        retry_counter++;
        last_backoff_time = (int) ( rand() / 1000 ) + last_backoff_time;
        PQfinish( conn );
        sleep( last_backoff_time );

        conn = PQconnectdb( conninfo );
    }

    while(
            (
                last_sql_state == NULL // Not inited (first run)
             || strcmp( last_sql_state, "57P01" ) == 0 // Terminated by Admin
             || strcmp( last_sql_state, "57014" ) == 0 // Canceled
            )
         && retry_counter < 3
         )
    {
        if( params == NULL )
        {
            result = PQexec( conn, query );
        }
        else
        {
            result = PQexecParams(
                conn,
                query,
                param_count,
                NULL,
                ( const char * const * ) params,
                NULL,
                NULL,
                1
            );
        }

        if(
            !(
                PQresultStatus( result ) == PGRES_COMMAND_OK ||
                PQresultStatus( result ) == PGRES_TUPLES_OK
             )
          )
        {
            fprintf(
                stderr,
                "Query failed: %s\n",
                PQerrorMessage( conn )
            );

            last_sql_state = PQresultErrorField( result, PG_DIAG_SQLSTATE );

            PQclear( result );

            retry_counter++;
        }
        else
        {
            return result;
        }
    }

    fprintf(
        stderr,
        "Query failed after %i tries.\n",
        retry_counter
    );

    return NULL;
}

void _handle_modification( char * channel, char * operation )
{
    // TODO: Locate massive memory leak in this function
    //  it only occurs in libpq-fe 10+
    PGresult * result;
    char * row_delta = "0";
    char * params[2];

    params[0] = operation;
    params[1] = channel;
    result = _execute_query( ( char * ) &log_modification, params, 2 );

    if( result == NULL )
    {
        fprintf(
            stderr,
            "INSERT on stats table failed: %s\n",
            PQerrorMessage( conn )
        );

        return;
    }

    PQclear( result );

    if( strcmp( operation, "DELETE" ) == 0 )
    {
        row_delta = "-1";
    }
    else if( strcmp( operation, "INSERT" ) == 0 )
    {
        row_delta = "1";
    }

    params[0] = row_delta;
    params[1] = channel;
    result = _execute_query( ( char * ) &update_stats, params, 2 );

    if( result == NULL )
    {
        fprintf(
            stderr,
            "UPDATE of row counts failed: %s\n",
            PQerrorMessage( conn )
        );

        return;
    }

    PQclear( result );
    return;
}

int main( int argc, char ** argv )
{
    PGnotify * notify;

    PGresult * result;
    PGresult * listen_result;

    int row_count    = 0;
    int i            = 0;
    int malloc_size  = 0;

    char * listen_command;
    char * schema_name;
    char * table_name;
    char * catalog_table_query =
        "    SELECT schema_name, "
        "           table_name "
        "      FROM dbstat.tb_catalog_table "
        "  ORDER BY table_name ";

    srand( 8675309 * time( 0 ) );
    parse_args( argc, argv );

    if( conninfo == NULL )
    {
        fprintf(
            stderr,
            "No way to connect to DB!\n"
        );
        return 1;
    }

    /* Connect to the database */
    conn = PQconnectdb( conninfo );

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
    result = _execute_query(
        catalog_table_query,
        NULL,
        0
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

        listen_result = _execute_query( listen_command, NULL, 0 );

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

    notify = NULL;

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
            if( DEBUG )
            {
                fprintf(
                   stdout,
                   "ASYNCRONOUS NOTIFY of '%s' received from backend PID %d with payload '%s'\n",
                   notify->relname, // Table name
                   notify->be_pid, // Backend PID
                   notify->extra // Operation
                );
            }

            _handle_modification( notify->relname, notify->extra );
            PQfreemem( notify );
        }
    }

    PQfinish( conn );
    return 0;
}
