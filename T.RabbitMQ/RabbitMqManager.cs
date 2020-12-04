using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using T.Models;

namespace T.RabbitMQ
{
    public class RabbitMqManager : IDisposable
    {
        private readonly RabbitMqPublisher _rabbitMqPublisher;

        private readonly RabbitMqConsumer _rabbitMqConsumer;

        public event EventHandler<ResultData> Received;

        #region Constructor

        public RabbitMqManager(string hostName)
        {
            ConnectionFactory connectionFactory = new ConnectionFactory { HostName = hostName };

            var connection = connectionFactory.CreateConnection();

            var channel = connection.CreateModel();

            _rabbitMqPublisher = new RabbitMqPublisher(connection, channel);

            _rabbitMqConsumer = new RabbitMqConsumer(connection, channel);

            _rabbitMqConsumer.Received += _rabbitMqConsumer_Received;
        }

        public RabbitMqManager(string hostName, string userName, string password)
        {
            ConnectionFactory connectionFactory = new ConnectionFactory { HostName = hostName, UserName = userName, Password = password };

            var connection = connectionFactory.CreateConnection();

            var channel = connection.CreateModel();

            _rabbitMqPublisher = new RabbitMqPublisher(connection, channel);

            _rabbitMqConsumer = new RabbitMqConsumer(connection, channel);

            _rabbitMqConsumer.Received += _rabbitMqConsumer_Received;
        }

        #endregion

        public bool Publish(object data, string queueName)
        {
            if (!string.IsNullOrEmpty(queueName)) return _rabbitMqPublisher.Publish(data, queueName);

            throw new Exception("Queue name can not be empty or null");
            //OnLog(LogTypes.Warning, "Publish", "Queue name can not be empty or null");
        }

        public void Subscribe(bool autoAck, string queueName)
        {
            if (!string.IsNullOrEmpty(queueName))
                _rabbitMqConsumer.Subscribe(autoAck, queueName);
            else
            {
                throw new Exception("Queue name can not be empty or null");
                //OnLog(LogTypes.Warning, "Subscribe", "Queue name can not be empty or null");
            }
        }

        private void _rabbitMqConsumer_Received(object sender, ResultData e)
        {
            Received?.Invoke(sender, e);
        }

        public void Dispose()
        {
            _rabbitMqConsumer.Dispose();

            _rabbitMqPublisher.Dispose();
        }
    }
}
