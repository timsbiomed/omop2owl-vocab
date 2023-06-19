.PHONY: all install python-dependencies download-dependencies docker-dependencies install-yarrrml-method help


# MAIN COMMANDS / GOALS ------------------------------------------------------------------------------------------------
all: n3c.db

n3c.owl n3c.db: io/input/termhub-csets
	 python3 -m n3c_owl_ingest

# SETUP / INSTALLATION -------------------------------------------------------------------------------------------------
python-dependencies:
	pip install -r requirements.txt

# TODO: allow some force updating of termhub-csets
io/input/termhub-csets:
	cd io/input
	git clone https://github.com/jhu-bids/termhub-csets.git
	cd termhub-csets
	git lfs pull

download-dependencies: io/input/termhub-csets

docker-dependencies:
	docker pull obolibrary/odkfull:dev

install-yarrrml-method: install
	docker pull rmlio/yarrrml-parser:latest
	docker pull rmlio/rmlmapper-java:v5.0.0

install: python-dependencies download-dependencies docker-dependencies

# HELP -----------------------------------------------------------------------------------------------------------------
help:
	@echo "-----------------------------------"
	@echo "	Command reference: N3C OWL Ingest"
	@echo "-----------------------------------"
	@echo "all"
	@echo "Creates all release artefacts.\n"
	@echo "n3c.owl"
	@echo "Creates OWL artefact: n3c.owl\n"
	@echo "n3c.db"
	@echo "Creates SemanticSQL sqlite artefact: n3c.db\n"
	@echo "install"
	@echo "Install dependencies. Everything needed for --method robot\n"
	@echo "install-yarrrml-method"
	@echo "Install dependencies. Everything needed for --method yarrrml, currently in development.\n"
