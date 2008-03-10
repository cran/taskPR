include Makevars
PARALLELP_OBJECTS=extra.o scheduler.o worker.o hashtable.o peval.o $(MPSPAWN)

all: spawnhelper taskPR.so

taskPR.so: $(PARALLELP_OBJECTS)
	$(SHLIB_LINK) -o $@ $(PARALLELP_OBJECTS) $(PKG_LIBS) $(PKG_CFLAGS)

spawnhelper: spawnhelper.o
	$(CC) spawnhelper.o -o spawnhelper $(CFLAGS) $(PKG_CFLAGS) $(PKG_LIBS)
	cp spawnhelper ../exec

clean:
	rm -f $(PARALLELP_OBJECTS) spawnhelper.o spawnhelper ../exec/spawnhelper taskPR.so

