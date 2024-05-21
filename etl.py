from crontab import CronTab
 
my_cron = CronTab(user='minh')
job = my_cron.new(command='python main.py')
job.minute.every(1)
 
my_cron.write()