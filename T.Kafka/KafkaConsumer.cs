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

        private string _topic = "";

        public KafkaConsumer(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public void StartConsume(string topic)
        {
            Console.WriteLine($"Consuming {topic}");

            _topic = topic;

            ThreadPool.QueueUserWorkItem(Consumer);
        }

        private void Consumer(object a)
        {
            var consumer = new Consumer(new ConsumerOptions(_topic, _brokerRouter));

            while (true)
            {
                foreach (var messages in consumer.Consume())
                {
                    string data = $"Response: PartitionId:{messages.Meta.PartitionId}, Offset:{ messages.Meta.Offset} Message:{ Encoding.UTF8.GetString(messages.Value)}";

                    Received?.Invoke(this, new ResultData() { Data = data, QueueName = _topic });
                }

                Thread.Sleep(5);
            }
        }

    }
}
