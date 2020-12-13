using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using T.Models;

namespace T.Kafka
{
    public class KafkaConsumer
    {
        public event EventHandler<ResultData> Received;

        private readonly IBrokerRouter _brokerRouter;

        private readonly string _topic;

        public KafkaConsumer(IBrokerRouter brokerRouter, string topic)
        {
            _brokerRouter = brokerRouter;

            _topic = topic;
        }

        public void Consume()
        {
            Console.WriteLine($"Consuming {_topic}");

            ThreadPool.QueueUserWorkItem(Consumer);
        }

        private void Consumer(object a)
        {
            var consumer = new Consumer(new ConsumerOptions(_topic, _brokerRouter));

            foreach (var messages in consumer.Consume())
            {
                string data = $"Response: PartitionId:{messages.Meta.PartitionId}, Offset:{ messages.Meta.Offset} Message:{ Encoding.UTF8.GetString(messages.Value)}";

                Received?.Invoke(this, new ResultData() { Data = data, QueueName = _topic });
            }
        }

    }
}
