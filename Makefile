
dl:
	mkdir -p data
	cd data && \
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Paper.rar && \
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Author.zip && \
	wget http://arnetminer.org/lab-datasets/aminerdataset/AMiner-Author2Paper.zip && \
	cd ../

extract:
	cd data && \
	unzip -o AMiner-Author.zip && \
	unzip -o AMiner-Author2Paper.zip && \
	cd ../ && \
	bash unrar.sh
	mv AMiner-Paper.txt data
