package main

import (
	"errors"
	"flag"
	msg "log"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
)

const timeout time.Duration = time.Millisecond * 500

//генерация случайной строки
var letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	var err error

	flagErrPtr := flag.Bool("getErrors", false, "Выгрузить ошибки")
	flag.Parse()

	msg.Println("starting...")

	redisdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	//проверка подключения к edis
	_, err = redisdb.Ping().Result()
	if err != nil {
		msg.Panic(err)
	}
	defer redisdb.Close()
	msg.Println("connected")

	if *flagErrPtr {
		list := redisdb.LRange("errors", 0, -1).Val()
		msg.Println("count:", len(list))
		for _, it := range list {
			msg.Println(it)
		}
		redisdb.Del("errors")
		return
	}

	//очистка списка кандидатов в лидеры
	redisdb.Del("leader")

	for {
		//пытаемся получить данные от мастера
		err = receiver(redisdb)
		msg.Println(err)

		//данных нет. выбираем нового мастера
		if isLeader(redisdb) {
			//мастер
			err = transmitter(redisdb)
			msg.Println(err)
		}
	}
}

func isLeader(db *redis.Client) bool {
	id := randString(16)
	msg.Println("id: ", id)

	//регистрируемся как кандидат в мастеры
	if db.RPush("leader", id) == nil {
		msg.Panic("fail self anonce for leader")
	}

	//ожидание регистрации остальных кандидатов.
	time.Sleep(timeout * 2)

	//выбор мастера.
	return db.Watch(func(tx *redis.Tx) error {
		_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
			msg.Println("candidate count: ", tx.LLen("leader").Val())

			leader := tx.LIndex("leader", 0)
			msg.Println("leader id: ", leader.Val())

			if leader != nil && leader.Val() == id {
				tx.Del("leader")
				return nil
			}
			return errors.New("slave")
		})
		return err
	}, "leader") == nil
}

func transmitter(db *redis.Client) error {
	msg.Println("I am leader")

	for {
		mess := randString(rand.Intn(80))
		_, err := db.RPush("messages", mess).Result()
		if err != nil {
			panic(err)
		}
		msg.Println("-> ", mess)
		time.Sleep(timeout)
	}
}

func receiver(db *redis.Client) error {
	msg.Println("I am slave")

	for {
		messages, err := db.BLPop(timeout*2, "messages").Result()
		if err != nil {
			//нет сообщений. переходим к выбору мастера
			return errors.New("timeout")
		}

		msg.Println("<- ", messages[1])
	}
}
