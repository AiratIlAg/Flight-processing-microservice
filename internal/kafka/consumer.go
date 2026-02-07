package kafka

import (
	"context"
	"encoding/json"
	"flight_processing/internal/models"
	"flight_processing/internal/repository"
	"log"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Consumer struct {
	//подключается к брокерам, читает топики, распред партиции, управляет офсетом
	group sarama.ConsumerGroup

	db         *pgxpool.Pool
	metaRepo   *repository.MetaRepository
	flightRepo *repository.FlightRepository
}

func NewConsumer(
	brokers []string,
	groupID string,
	db *pgxpool.Pool,
	metaRepo *repository.MetaRepository,
	flightRepo *repository.FlightRepository,
) (*Consumer, error) {

	cfg := sarama.NewConfig()

	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		group:      group,
		db:         db,
		metaRepo:   metaRepo,
		flightRepo: flightRepo,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, topics []string) {
	h := &handler{c: c}

	for {
		err := c.group.Consume(ctx, topics, h)
		if err != nil {
			log.Println("consumer error:", err)
		}
	}
}

type handler struct {
	c *Consumer
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(
	sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {

	//кафка отдаёт сообщения, мы читаем поток
	for msg := range claim.Messages() {

		var km FlightKafkaMessage
		if err := json.Unmarshal(msg.Value, &km); err != nil {
			log.Println("bad message:", err)
			continue // битые сообщения пропускаем
		}
		//обрабокта сообщений
		err := h.processMessage(sess.Context(), &km)
		if err != nil {
			log.Println("process error, will retry:", err)
			continue // НЕ коммитим offset → Kafka перечитает
		}
		//коммит только при успехе
		sess.MarkMessage(msg, "")
	}

	return nil
}

func (h *handler) processMessage(ctx context.Context, km *FlightKafkaMessage) error {
	//начинаем транзакцию
	tx, err := h.c.db.Begin(ctx)
	if err != nil {
		return err
	}
	//если что то упало
	defer tx.Rollback(ctx)

	fd := models.FlightData{
		AircraftType:    km.Request.AircraftType,
		FlightNumber:    km.Request.FlightNumber,
		DepartureDate:   km.Request.DepartureDate,
		ArrivalDate:     km.Request.ArrivalDate,
		PassengersCount: km.Request.PassengersCount,
	}

	// upsert flights
	err = h.c.flightRepo.Upsert(ctx, &fd)
	if err != nil {
		return err
	}

	// update meta status
	err = h.c.metaRepo.UpdateStatus(ctx, km.MetaID, "processed")
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
