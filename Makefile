dep:
	@docker compose up -d

run: #dep
	@go run main.go

ACTIONS_ARGS ?= "create,update,search,delete"
run-default:
	#@#if [[ "${ACTIONS_ARGS}" = "" ]]; then \
#    	go run default/main.go \
#    else \
#	    go run default/main.go --actions=${ACTIONS_ARGS} \
#    fi

    ifeq (${ACTIONS_ARGS},)
		go run default/main.go
    else
	    go run default/main.go --actions=${ACTIONS_ARGS}
    endif

BULK_ARGS ?= ""
run-bulk:
	@go run bulk/main.go ${BULK_ARGS}

clean:
	@docker compose down