# Generated by Django 4.1.3 on 2023-02-01 08:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datahub_v3_app', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='preaduit',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False)),
                ('database_name', models.CharField(max_length=15)),
            ],
            options={
                'db_table': 'preaduit',
            },
        ),
    ]
