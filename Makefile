
dl:
	mkdir -p data/original-data
	cd data/original-data && \
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Paper.rar && \
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Author.zip && \
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Author2Paper.zip && \
	cd ../

extract:
	cd data/original-data && \
	unzip -o AMiner-Author.zip && \
	unzip -o AMiner-Author2Paper.zip && \
	cd ../../ && \
	bash unrar.sh data/original-data/AMiner-Paper.rar data/original-data
