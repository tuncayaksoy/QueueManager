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
    public class KafkaPublisher
    {
        private readonly IBrokerRouter _brokerRouter;

        private string _topic = "";

        private string _message = "";

        public KafkaPublisher(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public void SendMessageAsync(string topic, string message)
        {
            _topic = topic;

            _message = message;

            ThreadPool.QueueUserWorkItem(Send);
        }

        private void Send(object a)
        {
            var producer = new Producer(_brokerRouter);

            producer.SendMessageAsync(_topic, new[] { new Message(_message), }).Wait();
        }
    }
}
