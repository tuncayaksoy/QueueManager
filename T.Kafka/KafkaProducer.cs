using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Protocol;

namespace T.Kafka
{
    public class KafkaProducer
    {
        private readonly IBrokerRouter _brokerRouter;

        public KafkaProducer(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public void Send(string topic, string message)
        {
            Producer producer = new Producer(_brokerRouter);

            producer.SendMessageAsync(topic, new[] { new Message(message) });//Wait
        }
    }
}
