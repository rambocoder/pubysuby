#for NUM in `seq 1 1 10`; do go test -v -race -parallel=1 -cover -covermode=atomic; done

for NUM in `seq 1 1 100`; do go test -v -race -parallel=10; done
