using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using T.Models;

namespace T.RabbitMQ
{
    internal class RabbitMqConsumer : IDisposable
    {
        public event EventHandler<ResultData> Received;

        private readonly IConnection _connection;

        private readonly IModel _channel;

        public RabbitMqConsumer(IConnection iConnection, IModel iModel)
        {
            _connection = iConnection;

            _channel = iModel;
        }

        /// <summary>
        /// Parametre değeri true ise, veriler alındığında kuyruktan silinir.
        /// </summary>
        /// <param name="autoAck"></param>
        /// <param name="queueName"></param>
        public void Subscribe(bool autoAck, string queueName)
        {
            try
            {
                var consumer = new EventingBasicConsumer(_channel);

                consumer.Received += Consumer_Received;

                _channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                _channel.BasicConsume(queueName, autoAck, consumer);
            }
            catch (Exception)
            {
                throw;
            }
        }

        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            var body = e.Body;

            string message = Encoding.UTF8.GetString(body.ToArray());

            Received?.Invoke(sender, new ResultData() { Data = message, QueueName = e.RoutingKey });
        }

        public void Dispose()
        {
            try
            {
                _channel.Close();

                _channel.Dispose();

                _connection.Close();

                _connection.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
