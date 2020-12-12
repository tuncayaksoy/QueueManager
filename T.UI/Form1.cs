using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using T.Kafka;
using T.Models;
using T.RabbitMQ;

namespace T.UI
{
    public partial class Form1 : Form
    {
        private RabbitMqManager _rabbitMqManager;
        private KafkaManager _kafkaManager;

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            cmbQueueType.SelectedIndex = 0;

            _rabbitMqManager = new RabbitMqManager("localHost");

            _rabbitMqManager.Received += _rabbitMqManager_Received;

            _kafkaManager = new KafkaManager("http://127.0.0.1:9092");

            _kafkaManager.Received += _kafkaManager_Received;
        }

        private void _rabbitMqManager_Received(object sender, ResultData e)
        {
            if (richTextBox1.InvokeRequired)
            {
                richTextBox1.Invoke((MethodInvoker)delegate
                {
                    richTextBox1.AppendText("Queue Name : " + e.QueueName + " \t" + " Data : " + e.Data);
                    richTextBox1.AppendText("\n");
                });
            }
            else
            {
                richTextBox1.AppendText("Queue Name : " + e.QueueName + " " + " Data : " + e.Data);
                richTextBox1.AppendText("\n");
            }
        }

        private void _kafkaManager_Received(object sender, ResultData e)
        {
            if (richTextBox1.InvokeRequired)
            {
                richTextBox1.Invoke((MethodInvoker)delegate
                {
                    richTextBox1.AppendText("Queue Name : " + e.QueueName + " \t" + " Data : " + e.Data);
                    richTextBox1.AppendText("\n");
                });
            }
            else
            {
                richTextBox1.AppendText("Queue Name : " + e.QueueName + " " + " Data : " + e.Data);
                richTextBox1.AppendText("\n");
            }
        }

        private void btnPublish_Click(object sender, EventArgs e)
        {
            if (cmbQueueType.SelectedIndex == 0)
            {
                try
                {
                    _rabbitMqManager.Publish(Guid.NewGuid(), txtPublishQueueName.Text);
                }
                catch (Exception exception)
                {
                    AddInfo(exception.Message);
                }
            }
            else
            {
                try
                {
                    _kafkaManager.Publish(Guid.NewGuid(), txtPublishQueueName.Text);
                }
                catch (Exception exception)
                {
                    AddInfo(exception.Message);
                }
            }
        }

        private void btnSubscribe_Click(object sender, EventArgs e)
        {
            if (cmbQueueType.SelectedIndex == 0)
            {
                try
                {
                    _rabbitMqManager.Subscribe(true, txtConsumerQueueName.Text);
                }
                catch (Exception exception)
                {
                    AddInfo(exception.Message);
                }
            }

            else
            {
                try
                {
                    _kafkaManager.Subscribe(txtConsumerQueueName.Text);
                }
                catch (Exception exception)
                {
                    AddInfo(exception.Message);
                }
            }
        }

        private void AddInfo(string info)
        {
            if (richTextBox1.InvokeRequired)
            {
                richTextBox1.Invoke((MethodInvoker)delegate
                {
                    richTextBox1.AppendText("Message : " + info);
                    richTextBox1.AppendText("\n");
                });
            }
            else
            {
                richTextBox1.AppendText("Message : " + info);
                richTextBox1.AppendText("\n");
            }
        }
    }
}
