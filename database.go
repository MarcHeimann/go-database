package vidatabase

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v4"
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

	return "Success", nil
}

func checkAndCreateDatabase() (string, error) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Error(os.Stderr, "Error acquiring connection:", err)
		os.Exit(1)
	}
	defer conn.Release()

	var checkDBStatement string = "SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('" + activecredentials.Database + "');"
	log.Info(checkDBStatement)
	var row pgx.Row = conn.QueryRow(context.Background(), checkDBStatement)

	log.Info(row)

	return "Success", nil
}

func CreateTableForAggregate(aggregateMap map[string]interface{}) error {

	log.Info(aggregateMap)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Error(os.Stderr, "Error acquiring connection:", err)
		os.Exit(1)
	}

	log.Info("Creating database table")

	var queryString string = "SELECT EXISTS ( SELECT FROM pg_catalog.pg_class c JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname = '#{activecredentials.Database}' AND    c.relname = '#{activecredentials.Table}' AND    c.relkind = 'r');"

	var row pgx.Row = conn.QueryRow(context.Background(), queryString)

	log.Info(row)

	defer conn.Release()

	return nil
}

func ConvertStructToMap(st interface{}) map[string]interface{} {

	reqRules := make(map[string]interface{})

	v := reflect.ValueOf(st)
	t := reflect.TypeOf(st)

	for i := 0; i < v.NumField(); i++ {
		key := strings.ToLower(t.Field(i).Name)
		typ := v.FieldByName(t.Field(i).Name).Kind().String()
		structTag := t.Field(i).Tag.Get("json")
		jsonName := strings.TrimSpace(strings.Split(structTag, ",")[0])
		value := v.FieldByName(t.Field(i).Name)

		// if jsonName is not empty use it for the key
		if jsonName != "" && jsonName != "-" {
			key = jsonName
		}

		if typ == "string" {
			if !(value.String() == "" && strings.Contains(structTag, "omitempty")) {
				fmt.Println(key, value)
				fmt.Println(key, value.String())
				reqRules[key] = value.String()
			}
		} else if typ == "int" {
			reqRules[key] = value.Int()
		} else {
			reqRules[key] = value.Interface()
		}

	}

	return reqRules
}
