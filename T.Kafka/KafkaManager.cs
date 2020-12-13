using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using T.Models;

namespace T.Kafka
{
    public class KafkaManager
    {
        private readonly KafkaProducer _kafkaProducer;

        public event EventHandler<ResultData> Received;

        private readonly KafkaOptions _kafkaOptions;

        public KafkaManager(string host)
        {
            _kafkaOptions = new KafkaOptions(new Uri(host));

            var brokerRouter = new BrokerRouter(_kafkaOptions);

            _kafkaProducer = new KafkaProducer(brokerRouter);
        }

        public KafkaManager(string host, string userName, string name)
        {

        }

        public KafkaConsumer CreateConsumer(string topic)
        {
            var brokerRouter = new BrokerRouter(_kafkaOptions);

            return new KafkaConsumer(brokerRouter, topic);
        }

        public void Send(string topic, string message)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new Exception("Queue name can not be empty or null");
                //OnLog(LogTypes.Warning, "Publish", "Queue name can not be empty or null");
            }
            else
            {
                _kafkaProducer.Send(topic, message);
            }
        }

        public void Subscribe(KafkaConsumer kafkaConsumer)
        {
            kafkaConsumer.Received += KafkaConsumer_Received;

            kafkaConsumer.Consume();
        }

        private void KafkaConsumer_Received(object sender, ResultData e)
        {
            Received?.Invoke(sender, e);
        }
    }
}
