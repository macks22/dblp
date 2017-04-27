DATA_DIR=data/original-data


all: deps config extract

deps:
	pip install -r requirements.txt

config:
	sed 's?/data/username/aminer-network?'`pwd`'?' pipeline/config-example.py > pipeline/config.py
	python verify_config.py

$(DATA_DIR)/AMiner-Paper.rar:
	mkdir -p $(DATA_DIR)
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Paper.rar -P $(DATA_DIR)

$(DATA_DIR)/AMiner-Author.zip:
	mkdir -p $(DATA_DIR)
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Author.zip -P $(DATA_DIR)

$(DATA_DIR)/AMiner-Author2Paper.zip:
	mkdir -p $(DATA_DIR)
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Author2Paper.zip -P $(DATA_DIR)

dl: $(DATA_DIR)/AMiner-Paper.rar $(DATA_DIR)/AMiner-Author.zip $(DATA_DIR)/AMiner-Author2Paper.zip

$(DATA_DIR)/Aminer-Paper.txt:
	bash unrar.sh $(DATA_DIR)/AMiner-Paper.rar $(DATA_DIR)

$(DATA_DIR)/AMiner-Author.txt:
	unzip -o $(DATA_DIR)/AMiner-Author.zip -d $(DATA_DIR)

$(DATA_DIR)/AMiner-Author2Paper.txt:
	unzip -o $(DATA_DIR)/AMiner-Author2Paper.zip -d $(DATA_DIR)

extract: dl $(DATA_DIR)/AMiner-Paper.txt $(DATA_DIR)/AMiner-Author.txt $(DATA_DIR)/AMiner-Author2Paper.txt
	python verify_download.py
