package vidatabase

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

type DatabaseCredentials struct {
	User     string
	Password string
	Url      string
	Port     string
	Database string
	Table    string
}

var activecredentials DatabaseCredentials

var pool *pgxpool.Pool

func ConnectToDatabase(d DatabaseCredentials) (string, error) {

	var databaseUrl string = "postgres://" + d.User + ":" + d.Password + "@" + d.Url + ":" + d.Port + "/" + d.Database + ""
	fmt.Println(databaseUrl)
	var err error
	pool, err = pgxpool.Connect(context.Background(), databaseUrl)
	if err != nil {
		log.Error("Unable to connect to database, postgres://****:****@" + d.Url + ":" + d.Port + "/" + d.Database + "")
		return "Unable to connect to database, postgres://****:****@" + d.Url + ":" + d.Port + "/" + d.Database + "", err
	} else {
		log.Info("Successfully connected to, postgres://****:****@" + d.Url + ":" + d.Port + "/" + d.Database + "")
	}

	activecredentials = d
	checkAndCreateDatabase()

	log.Info("********* NEW MESSAGE ******")

	return "Success", nil
}

func checkAndCreateDatabase() (string, error) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error acquiring connection:", err)
		os.Exit(1)
	}
	defer conn.Release()

	var checkDBStatement string = "SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('" + activecredentials.Database + "');"
	log.Info(checkDBStatement)
	// var statement string = "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema');"

	// err = conn.QueryRow(context.Background(), "select name from widgets where id=$1", 42).Scan(&name, &weight)
	conn.QueryRow(context.Background(), checkDBStatement)

	return "Success", nil
}
