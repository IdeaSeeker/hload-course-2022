package dqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"strconv"
	"time"
	"github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
)

/*
 * Можно создавать несколько очередей
 *
 * Для клиента они различаются именами
 * 
 * В реализации они могут потребовать вспомогательных данных
 * Для них - эта структура. Можете определить в ней любые поля 
 */
type DQueue struct {
	name                string
	nShards             int
	currentShardForPush int
	shards              []*redis.Client
}

type ZkNodeData struct {
	nShards       int
	currentPushId int
	version       int32
}

// config
var cnxt = context.Background()
var redisClients []*redis.Client
var zkClusters = []string{}

var zkHead = "/zookeeper/nikita"
var timeLayout = "2001-01-01T01:01:01"

/*
 * Запомнить данные и везде использовать
 */
func Config(redisOptions *[]*redis.Options, zkCluster []string) {
	redisClients = []*redis.Client{}
	for _, opts := range *redisOptions {
		redisClients = append(redisClients, redis.NewClient(opts))
	}
	zkClusters = zkCluster
}

/*
 * Открываем очередь на nShards шардах
 *
 * Попытка создать очередь с существующим именем и другим количеством шардов
 * должна приводить к ошибке
 *
 * При попытке создать очередь с существующим именем и тем же количеством шардов
 * нужно вернуть экземпляр DQueue, позволяющий делать Push/Pull
 *
 * Предыдущее открытие может быть совершено другим клиентом, соединенным с любым узлом
 * Redis-кластера
 *
 * Отдельные узлы Redis-кластера могут выпадать. Availability очереди в целом
 * не должна от этого страдать
 *  
 */
func Open(name string, nShards int) (DQueue, error) {
	if nShards > len(redisClients) {
		return DQueue{}, errors.New("error: nShards > len(allShards)")
	}

	conn, _, err := zk.Connect(zkClusters, time.Second)
	if err != nil {
		return DQueue{}, err
	}
	defer conn.Close()

	headExists, _, err := conn.Exists(zkHead)
	if err != nil {
		return DQueue{}, err
	}

	if !headExists {
		conn.Create(zkHead, []byte("hi"), 0, zk.WorldACL(zk.PermAll))
	}

	err = Lock(conn, zkHead)
	if err != nil {
		return DQueue{}, err
	}

	queuePath := zkHead + "/" + name
	queueExists, _, err := conn.Exists(queuePath)
	if err != nil {
		return DQueue{}, err
	}

	if queueExists {
		nodeDataBytes, stat, err := conn.Get(queuePath)
		if err != nil {
			return DQueue{}, err
		}

		nodeData, err := getNodeData(string(nodeDataBytes), stat)
		if err != nil {
			return DQueue{}, err
		}

		if nodeData.nShards != nShards {
			return DQueue{}, errors.New("error: nodeData.nShards != nShards")
		}

		return DQueue{name, nShards, 0, redisClients[:nShards]}, nil
	}

	_, err = conn.Create(queuePath, []byte(fmt.Sprintf("%d|%d", nShards, 0)), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return DQueue{}, err
	}

	return DQueue{name, nShards, 0, redisClients[:nShards]}, nil
}

/*
 * Пишем в очередь. Каждый следующий Push - в следующий шард
 * 
 * Если шард упал - пропускаем шард, пишем в следующий по очереди
 */
func (q *DQueue) Push(value string) error {
	conn, _, err := zk.Connect(zkClusters, time.Second * 10)
	if err != nil {
		return err
	}
	defer conn.Close()

	queuePath := zkHead + "/" + q.name
	err = Lock(conn, queuePath)
	if err != nil {
		return err
	}

	nodeDataBytes, stat, err := conn.Get(queuePath)
	if err != nil {
		return err
	}

	nodeData, err := getNodeData(string(nodeDataBytes), stat)
	if err != nil {
		return err
	}

	for i := 0; i < q.nShards; i++ {
		currentTime := time.Now().Format(timeLayout)
		valueWithTime := value + "::" + currentTime

		currentShard := q.shards[nodeData.currentPushId]
		nodeData.currentPushId = (nodeData.currentPushId + 1) % nodeData.nShards

		err := currentShard.RPush(cnxt, q.name, valueWithTime).Err()
		if err != nil && err != redis.Nil {
			continue
		}

		return pushNodeData(nodeData, conn, queuePath)
	}

	return errors.New("error: Push failed")
}

/*
 * Читаем из очереди
 *
 * Из того шарда, в котором самое раннее сообщение
 *
 */
func (q *DQueue) Pull() (string, error) {
	conn, _, err := zk.Connect(zkClusters, time.Second * 10)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	queuePath := zkHead + "/" + q.name
	err = Lock(conn, queuePath)
	if err != nil {
		return "", err
	}

	resultValue, resultShardIndex := "", -1
	minTime, _ := time.Parse(timeLayout, timeLayout)

	for i := 0; i < q.nShards; i++ {
		currentShard := q.shards[i]

		valueWithTime, err := currentShard.LRange(cnxt, q.name, 0, 0).Result()
		if err != nil && err != redis.Nil || len(valueWithTime) == 0 {
			continue
		}
		tupleValueTime := strings.Split(valueWithTime[0], "::")
		value := tupleValueTime[0]
		pushTimeString := tupleValueTime[1]

		pushTime, err := time.Parse(timeLayout, pushTimeString)
		if err != nil {
			return "", err
		}

		if pushTime.Before(minTime) {
			resultValue = value
			resultShardIndex = i
			minTime = pushTime
		}
	}

	if resultShardIndex != -1 {
		_, err := q.shards[resultShardIndex].LPop(cnxt, q.name).Result()
		return resultValue, err
	}

	return "", errors.New("error: Pull failed")
}

func Lock(conn *zk.Conn, seqNodePath string) error {
	locknodePath := seqNodePath + "/" + "_locknode"
	locknodeExists, _, err := conn.Exists(locknodePath)
	if err != nil {
		return err
	}

	if !locknodeExists {
		_, err := conn.Create(locknodePath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	seqNodePath, err = conn.Create(
		locknodePath + "/" + "lock",
		[]byte{},
		zk.FlagSequence | zk.FlagEphemeral,
		zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	seqIndex, err := getIndexFromSeqNode(seqNodePath)
	if err != nil {
		return err
	}

	for {
		children, _, err := conn.Children(locknodePath)
		if err != nil {
			return err
		}

		minChildSeqIndex, minChildrenIndex := seqIndex, -1
		for index, childPath := range children {
			childSeqIndex, err := getIndexFromSeqNode(childPath)
			if err != nil {
				return err
			}

			if childSeqIndex < minChildSeqIndex {
				minChildrenIndex = index
				minChildSeqIndex = childSeqIndex
			}
		}

		if minChildSeqIndex >= seqIndex {
			return nil
		}

		exists, _, event, err := conn.ExistsW(children[minChildrenIndex])
		if err != nil {
			return err
		}

		if exists {
			_ = <-event
		}
	}
}

// internal

func getNodeData(nodeData string, stat *zk.Stat) (ZkNodeData, error) {
	splitted := strings.Split(nodeData, "|")

	nShards, err := strconv.Atoi(splitted[0])
	if err != nil {
		return ZkNodeData{}, err
	}

	currentPushId, err := strconv.Atoi(splitted[1])
	if err != nil {
		return ZkNodeData{}, err
	}

	return ZkNodeData{currentPushId: currentPushId, nShards: nShards, version: stat.Version}, nil
}

func pushNodeData(data ZkNodeData, c *zk.Conn, queuePath string) error {
	nodeData := fmt.Sprintf("%d|%d", data.nShards, data.currentPushId)
	_, err := c.Set(queuePath, []byte(nodeData), data.version)
	return err
}

func getIndexFromSeqNode(path string) (int, error) {
	const indexLength = 10
	return strconv.Atoi(path[len(path) - indexLength:])
}
