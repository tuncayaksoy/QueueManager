using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace T.RabbitMQ
{
    internal class RabbitMqPublisher : IDisposable
    {
        private readonly IConnection _connection;

        private readonly IModel _channel;

        public RabbitMqPublisher(IConnection iConnection, IModel iModel)
        {
            _connection = iConnection;

            _channel = iModel;
        }

        public bool Publish(object data, string queueName)
        {
            try
            {
                string jsonData = JsonConvert.SerializeObject(data);

                _channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                byte[] body = Encoding.UTF8.GetBytes(jsonData);

                _channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: body);

                Console.WriteLine("Message has sent. Message : " + data);

                return true;
            }
            catch (Exception)
            {
                throw;
            }
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
