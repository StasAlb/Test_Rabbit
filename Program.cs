using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.MessagePatterns;
using RabbitMQ.Client.Events;

namespace Test_Rabbit
{
    class Program
    {
        private static IConnection connection;
        private static IModel model;
        static void Main()
        {
            try
            {
                string rabbitExchange = System.Configuration.ConfigurationManager.AppSettings["rabbitExchange"];
                string rabbitQueue = System.Configuration.ConfigurationManager.AppSettings["rabbitQueue"];
                string rabbitRoutingKey = System.Configuration.ConfigurationManager.AppSettings["rabbitRoutingKey"];
                CardPersoMessage cardPersoMessage = new CardPersoMessage()
                {
                    CardId = "12346",
                    BranchCode = "4208",
                    DeliverCode = "4208",
                    CardStatus = "8",
                    FioEmployee = "so user",
                    Date = DateTime.Now
                };
                connection = makeConnection();
                model = GetRabbitChannel(rabbitExchange, rabbitQueue, rabbitRoutingKey);
                var serializer = new Newtonsoft.Json.JsonSerializer();
                var stringWriter = new StringWriter();
                using (var writer = new Newtonsoft.Json.JsonTextWriter(stringWriter))
                {
                    writer.QuoteName = false;
                    writer.Formatting = Formatting.None;
                    serializer.Serialize(writer, cardPersoMessage);
                }

                string mes = stringWriter.ToString();
                byte[] message = Encoding.UTF8.GetBytes(mes);
                model.BasicPublish(rabbitExchange, rabbitRoutingKey, null, message);
                Console.WriteLine("Message send");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                model?.Close();
                connection?.Close();
                Console.WriteLine("Press any key...");
                Console.ReadKey();
            }
        }

        static IConnection makeConnection()
        {
            ConnectionFactory factory = new ConnectionFactory()
            {
                UserName = System.Configuration.ConfigurationManager.AppSettings["rabbitUser"],
                Password = System.Configuration.ConfigurationManager.AppSettings["rabbitPassword"],
                VirtualHost = System.Configuration.ConfigurationManager.AppSettings["rabbitHost"],
                HostName = System.Configuration.ConfigurationManager.AppSettings["rabbitAddress"]
            };
            IConnection conn = factory.CreateConnection();
            return conn;
        }

        static IModel GetRabbitChannel(string exchangeName, string queueName, string routingKey)
        {
            IModel model = connection.CreateModel();
            model.ExchangeDeclare(exchangeName, ExchangeType.Direct);
            model.QueueDeclare(queueName, false, false, false, null);
            model.QueueBind(queueName, exchangeName, routingKey, null);
            return model;
        }

    }
    class CardPersoMessage
    {
        public string CardId;
        public string BranchCode;
        public string DeliverCode;
        public string CardStatus;
        public string FioEmployee;
        public DateTime Date;
    }
}
