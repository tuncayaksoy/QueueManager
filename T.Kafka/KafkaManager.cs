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
        private readonly KafkaPublisher _kafkaPublisher;

        private readonly KafkaConsumer _kafkaConsumer;

        public event EventHandler<ResultData> Received;


        //var kafkaOptions = new KafkaOptions(new Uri("http://192.168.99.100:9092"));
        //var brokerRouter = new BrokerRouter(kafkaOptions);
        //var producer = new TestProducer(brokerRouter);

        //Console.WriteLine("Send a Message to TestTopic:");
        //while (true)
        //{
        //    producer.SendMessageAsync("TestTopic", Console.ReadLine());
        //}

        public KafkaManager(string host)
        {
            var kafkaOptions = new KafkaOptions(new Uri(host));

            var brokerRouter = new BrokerRouter(kafkaOptions);

            _kafkaPublisher = new KafkaPublisher(brokerRouter);

            _kafkaConsumer = new KafkaConsumer(brokerRouter);

            _kafkaConsumer.Received += _kafkaConsumer_Received;
        }

        private void _kafkaConsumer_Received(object sender, ResultData e)
        {
            Received?.Invoke(sender, e);
        }

        public KafkaManager(string host, string userName, string name)
        {

        }

        public void Publish(object data, string queueName)
        {
            if (string.IsNullOrEmpty(queueName))
            {
                throw new Exception("Queue name can not be empty or null");
                //OnLog(LogTypes.Warning, "Publish", "Queue name can not be empty or null");
            }
            else
            {
                _kafkaPublisher.SendMessageAsync(queueName, data.ToString());
            }
        }

        public void Subscribe(string queueName)
        {
            if (!string.IsNullOrEmpty(queueName))
                _kafkaConsumer.StartConsume(queueName);
            else
            {
                throw new Exception("Queue name can not be empty or null");
                //OnLog(LogTypes.Warning, "Subscribe", "Queue name can not be empty or null");
            }
        }
    }
}
