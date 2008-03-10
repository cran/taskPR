/* ****************************************************************************
*   Copyright (C) 2004  Oak Ridge National Laboratory
*
*   This program is free software; you can redistribute it and/or
*   modify it under the terms of the GNU General Public License
*   as published by the Free Software Foundation, version 2.
*
*   This program is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.
*
*   You should have received a copy of the GNU General Public License
*   along with this program; if not, write to the Free Software
*   Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
**************************************************************************** */
/* "Extra" functions for the Parallel-R prototype R package.
 *  These are functions that are needed for the package, but are either not
 *  needed by the main version, or are embedded into R's code in the main
 *  version.
 */
/*  $Author: david $
    $Date: 2008-03-02 03:44:21 $
    $Revision: 1.15 $
 */
#include <unistd.h>
//#define USE_RINTERNALS
#include <R.h>
#include <Rinternals.h>
//#include "Defn.h"
//#include "Rconnections.h"
#include "peval.h"

char **FindInputs( SEXP sxIn );
SEXP PackageVersionOfCheckAndConvert(SEXP sxIn);

/* From "memory.c" */
SEXP (RLENGTH)(SEXP x) { return ScalarInteger(LENGTH(x)); }
SEXP (RStrLength)(SEXP x) {
	if (TYPEOF(x) == STRSXP) return ScalarInteger(LENGTH(STRING_ELT(x, 0)));
	else return ScalarInteger(-1);
}

// SEXP mkPRIMSXP(int offset, int eval);
// int StrToInternal(const char *s);
// SEXP do_remove(SEXP call, SEXP op, SEXP args, SEXP rho);

int my_remove(char *cpName, SEXP rho) {
	warning("WARNING:  stub function my_remove called on variable %s\n", cpName);
	return 0;
}

#if 0
/* From "envir.c" */
int my_remove(char *cpName, SEXP rho) {
	SEXP call = R_NilValue;
	SEXP op;
	SEXP args;

	PROTECT( op = mkPRIMSXP(StrToInternal("remove"), BUILTINSXP) );
	PROTECT( args = CONS(mkString(cpName), CONS(rho, CONS(ScalarLogical(0), R_NilValue))));
	do_remove(call, op, args, rho);
	
	UNPROTECT( 2 );

	return 0;
}
#endif

#if 0
/* From "connections.c" */
SEXP ClearConn(SEXP sxCon) {
	Rconnection con = getConnection(asInteger(sxCon));
	int iBlocking = con->blocking;
	int ipBuffer[4096];

	con->blocking = 0;
	do {
		con->read(ipBuffer, sizeof(int), 4096, con);
	} while (con->incomplete != TRUE);

	con->blocking = iBlocking;

	return R_NilValue;
}
#endif

/* From "names.c" */
SEXP Rinstall(SEXP s) {
	return install(CHAR(STRING_ELT(s, 0)));
}

/* From "eval.c" */

static int iSchedulerSocket = 0;

/* Startup/Setup the parallel engine.  This function is called by the R
 * function "StartPE", and it calls the InitializeParallelExecution function in
 * peval.c to do all of the work.
 */
SEXP EnableParallelExecution(SEXP sxIn, SEXP rho) {
	extern int iGlobalParallelEngineEnabled;

	if (TYPEOF(sxIn) != INTSXP && TYPEOF(sxIn) != REALSXP) {
		error("Number of worker processes must be a number");
	}

	if (iGlobalParallelEngineEnabled) {
		error("Parallel Engine already enabled");
	}

	sxIn = coerceVector(sxIn, INTSXP);

	if (InitializeParallelExecution(INTEGER(sxIn)[0], &iSchedulerSocket,
				rho) != 0)
		error("Failed to initialize the parallel engine");

	iGlobalParallelEngineEnabled = 1;

	return R_NilValue;
}

/* A wrapper around the above function which takes the verbose flag from the
 * R script and sets the internal verbose level.
 * This function and the above function should probably be combined together.
 */
SEXP EnableVerboseParallelExecution(SEXP sxIn, SEXP sxLevel, SEXP rho) {
    extern int iGlobalParallelEngineEnabled;
    int iLevel = 2;
                                                                                
    if (sxLevel != R_NilValue) {
        if (TYPEOF(sxLevel) != INTSXP && TYPEOF(sxLevel) != REALSXP) {
            error("Verbose level must be a number");
        }
                                                                                
        sxLevel = coerceVector(sxLevel, INTSXP);
        iLevel = INTEGER(sxLevel)[0];
        if (iLevel < 1 || iLevel > 3) error("Verbose level should be 1-3");
    }
                                                                                
    if (iGlobalParallelEngineEnabled) {
        iGlobalParallelEngineEnabled = iLevel;
        return R_NilValue;
    }
                                                                                
    EnableParallelExecution(sxIn, rho);
                                                                                
    iGlobalParallelEngineEnabled = iLevel;
                                                                                
    return R_NilValue;
}

/* Stop the parallel engine.  Called by the StopPE R script.
 * The worker processes and all extra threads exit their respective work loops.
 */
SEXP DisableParallelExecution(void) {
    extern int iGlobalParallelEngineEnabled;
    void *vp = NULL;
    int iTemp;
                                                                                
    if (!iGlobalParallelEngineEnabled) {
        error("Parallel Engine does not appear to be running");
    }
                                                                                
    /* First, wait for all jobs to be finished, and then signal the
     * scheduler to exit.  Wait for the signal from the scheduler that
     * it has actually finished.
     * (The scheduler won't exit until all jobs are finished, but
     *  this takes care of the issue of variables being returned
     *  to R's workspace. )
     */
    WaitForVariable(NULL, iSchedulerSocket, R_GlobalEnv);
    write(iSchedulerSocket, &vp, sizeof(void *));
    read(iSchedulerSocket, &iTemp, sizeof(int));
                                                                                
    iGlobalParallelEngineEnabled = 0;
                                                                                
    return R_NilValue;
}

/* Perform a "PE" call.  This code should be shorter and simpler than the
 * version in the eval function in the main-line code.
 */
SEXP CallPE( SEXP sxCall, SEXP sxGlobal, SEXP rho ) {
    extern int iGlobalParallelEngineEnabled;

	/* Decompose0(sxCall); */

	if (TYPEOF(sxCall) != LANGSXP) {
		error("CallPE: Expected LANGSXP object");
	}

	if (iGlobalParallelEngineEnabled) {
		char **cppInputList;
		int iIndex = 0;
		char cpBuf[256];
		int iStrLen;
		int iGlobal = 0;

		sxCall = PackageVersionOfCheckAndConvert(sxCall);
		if (isLogical(sxGlobal)) iGlobal = LOGICAL(sxGlobal)[0];

		cppInputList = FindInputs(CDR(CDR(sxCall)));
		if (cppInputList != NULL && iGlobalParallelEngineEnabled > 1) {
			const char *cpOutputName = CHAR(PRINTNAME(CAR(CDR(sxCall))));
			sprintf(cpBuf, "%s = f(", cpOutputName);
			while (cppInputList[iIndex] != NULL) {
				iStrLen = strlen(cpBuf);
				if (iIndex == 0)
					iStrLen += snprintf(cpBuf + iStrLen, 256 - iStrLen,
							" %s", cppInputList[iIndex]);
				else
					iStrLen += snprintf(cpBuf + iStrLen, 256 - iStrLen,
							", %s", cppInputList[iIndex]);
				free(cppInputList[iIndex]);
				iIndex++;
			}
			free(cppInputList);
			printf("Found parallel exec instruction: %s )\n", cpBuf);
		} else if (cppInputList == NULL) {
			printf("Found parallel exec instruction: No inputs?\n");
		}

		ParallelExecute(sxCall, iSchedulerSocket, iGlobal, rho);

		return R_NilValue;

	} else {
		printf("Warning: ParallelExecution not currently enabled.\n");
		return eval(sxCall, rho);
	}

	return R_NilValue;	/* Not reached */
}

/* Perform a "POBJ" call.  It is basically a wrapper around WaitForVariable. */
SEXP CallPOBJ(SEXP sxCall, SEXP rho) {
    extern int iGlobalParallelEngineEnabled;

	if (!iGlobalParallelEngineEnabled) {
		warning("CallPOBJ: ParallelExecution not enabled");
		return R_NilValue;
	}
	if (TYPEOF(sxCall) == SYMSXP) {
		WaitForVariable(CHAR(PRINTNAME(sxCall)),
				iSchedulerSocket, rho);
		return sxCall;
	}

	error("Input of a SYMSXP not found");

	return R_NilValue;	/* Not reached */
}

/* 'Lite' version of the CheckAndConvert function from "eval.c".
 * Only has step 3.
 */
SEXP PackageVersionOfCheckAndConvert(SEXP sxIn) {
	SEXP sxTmp;

    if (TYPEOF(CAR(CDR(sxIn))) == LANGSXP) {
        /* The input is in the form "g(a) <- f(...)", so convert it. */
        sxTmp = CAR(CDR(CAR(CDR(sxIn))));   /* Should be "a" */
        sxIn = LCONS(install("{"), CONS(sxIn, CONS(sxTmp, R_NilValue)));
        sxIn = LCONS(install("<-"), CONS(sxTmp, CONS(sxIn, R_NilValue)));
    }

	return sxIn;
}

char **FindInputs1( SEXP sxIn, char **cppCurrentList, int *ipListSize);
                                                                                
/* Given the starting SEXP (should be a LANGSXP) for a function call,
 * create a (null terminated) list of input variables.
 */
char **FindInputs( SEXP sxIn ) {
    int iListSize = 0;
    char **cppCurrentList;
    if (TYPEOF(sxIn) != LANGSXP && TYPEOF(sxIn) != LISTSXP) return NULL;
                                                                                
    cppCurrentList = FindInputs1(sxIn, NULL, &iListSize);
    cppCurrentList = realloc(cppCurrentList, sizeof(char *) * (1 + iListSize));
    cppCurrentList[iListSize] = NULL;
    return cppCurrentList;
}
char **FindInputs1( SEXP sxIn, char **cppCurrentList, int *ipListSize) {

    if (sxIn == NULL || sxIn == R_NilValue) return cppCurrentList;
                                                                                
    switch(TYPEOF(sxIn)) {
        case 2: /* LISTSXP */
            /* printf("FindInputs1:%d: Found list\n", *ipListSize); */
            if (TYPEOF(CAR(sxIn)) == SYMSXP) {
                const char *cpName = CHAR(PRINTNAME(CAR(sxIn)));
                /* Check to see if the name is empty. */
                if (cpName != NULL && *cpName != '\0') {
                    /* Check to see if the name is already on the list. */
                    int i;
                    for (i = 0; i < *ipListSize; i++) {
                        if (strcmp(cpName, cppCurrentList[i]) == 0)
                            break;
                    }
                    if (i == *ipListSize) {
                        cppCurrentList = realloc(cppCurrentList,
                                sizeof(char *) * (1 + *ipListSize));
                        cppCurrentList[(*ipListSize)++] =
                                strdup(CHAR(PRINTNAME(CAR(sxIn))));
                    }
                }
            } else {
                cppCurrentList = FindInputs1(CAR(sxIn), cppCurrentList,
                        ipListSize);
            }
            return (FindInputs1(CDR(sxIn), cppCurrentList, ipListSize));
            break;
        case 3: /* CLOSXP */    /* Fall through */
        case 6: /* LANGSXP */
            /* printf("FindInputs1:%d: Found lang\n", *ipListSize); */
            /* If the function is the "$" function, then ignore its second
             * parameter (there shouldn't be any after the second).
             */
            if (strcmp(CHAR(PRINTNAME(CAR(sxIn))), "$") == 0) {
                if (TYPEOF(CAR(CDR(sxIn))) != LANGSXP) {
                    PROTECT(sxIn = CONS(CAR(CDR(sxIn)), R_NilValue));
                    cppCurrentList = FindInputs1(sxIn, cppCurrentList,
                            ipListSize);
                    UNPROTECT( 1 );
                    return cppCurrentList;
                }
                return FindInputs1(CAR(CDR(sxIn)), cppCurrentList, ipListSize);
            }
                                                                                
            cppCurrentList=FindInputs1(CAR(sxIn), cppCurrentList, ipListSize);
            return (FindInputs1(CDR(sxIn), cppCurrentList, ipListSize));
            break;  /* Not reached */
        default:
            break;
    }
    return cppCurrentList;
}

/* **********************************************************************
 * *********************************************************************/
/* From sockconn.c */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>

/* Open a tcp/ip socket, bind it to the given port, and start listening.
 * sBlock is a boolean saying whether the socket should be blocking or not.
 * sBacklog sets the maximum number of connections waiting to be accepted.
 * Note:  The backlog parameter is often lowered by the OS!
 *        See the listen (2) man page for details.
 */
SEXP OpenSocketAndListen(SEXP sPort, SEXP sBlock, SEXP sBacklog) {
    int iSocket;
    struct sockaddr_in ssAddr;
    int iOne = 1;

    sPort = coerceVector(sPort, INTSXP);
    sBlock = coerceVector(sBlock, LGLSXP);
    sBacklog = coerceVector(sBacklog, INTSXP);

    iSocket = socket(PF_INET, SOCK_STREAM, 0);
    if (iSocket == -1) error("Failed to open socket");

    setsockopt(iSocket, SOL_SOCKET, SO_REUSEADDR, &iOne, sizeof(iOne));

    /* Blocking should be the default behavior, so only the non-blocking
     * case has to be set.
     */
    if (! INTEGER(sBlock)[0])
        fcntl(iSocket, F_SETFL, O_NONBLOCK);

    memset(&ssAddr, 0x00, sizeof(ssAddr));
    ssAddr.sin_family = AF_INET;
    ssAddr.sin_port = htons(INTEGER(sPort)[0]);
    ssAddr.sin_addr.s_addr = INADDR_ANY;
    if (bind(iSocket, (struct sockaddr *) &ssAddr, sizeof(ssAddr)) == -1)
        error("Failed to bind to port.  Already in use?");

    if (listen(iSocket, INTEGER(sBacklog)[0]) == -1)
        error("Failed to listen to port.");

    return (ScalarInteger(iSocket));
}

/* Given an open, listening socket, accept (in a blocking manner) a
 * connection on that socket, and return the new FD.
 */
SEXP AcceptConnFromSocket(SEXP sSocket) {
    int iSocket;

    iSocket = INTEGER(coerceVector(sSocket, INTSXP))[0];

    return (ScalarInteger(accept(iSocket, NULL, NULL)));
}

/* Close the given socket (or whatever the given file descriptor goes with).
 */
SEXP CloseSocket(SEXP sSocket) {
    int iSocket;

    iSocket = INTEGER(coerceVector(sSocket, INTSXP))[0];

    return (ScalarInteger(close(iSocket)));
}

/* **********************************************************************
 * *********************************************************************/
/* From serialize.c */
typedef size_t R_size_t;

typedef struct membuf_st {
	R_size_t size;
	R_size_t count;
	unsigned char *buf;
} *membuf_t;

static void resize_buffer(membuf_t mb, R_size_t needed)
{
	/* This used to allocate double 'needed', but that was problematic for
	 *        large buffers */
	R_size_t newsize = needed;
	/* we need to store the result in a RAWSXP */
	if(needed > INT_MAX)
		error("serialization is too large to store in a raw vector");
	mb->buf = realloc(mb->buf, newsize);
	if (mb->buf == NULL)
		error("cannot allocate buffer");
	mb->size = newsize;
}

static int InCharMem(R_inpstream_t stream)
{
	membuf_t mb = stream->data;
	if (mb->count >= mb->size)
		error("read error");
	return mb->buf[mb->count++];
}

static void InBytesMem(R_inpstream_t stream, void *buf, int length)
{
	membuf_t mb = stream->data;
	if (mb->count + (R_size_t) length > mb->size)
		error("read error");
	memcpy(buf, mb->buf + mb->count, length);
	mb->count += length;
}

static void OutCharMem(R_outpstream_t stream, int c)
{
	membuf_t mb = stream->data;
	if (mb->count >= mb->size)
		resize_buffer(mb, mb->count + 1);
	mb->buf[mb->count++] = c;
}

static void OutBytesMem(R_outpstream_t stream, void *buf, int length)
{
	membuf_t mb = stream->data;
	R_size_t needed = mb->count + (R_size_t) length;
	/* There is a potential overflow here on 32-bit systems */
	if((double) mb->count + length > (double) INT_MAX)
		error("serialization is too large to store in a raw vector");
	if (needed > mb->size) resize_buffer(mb, needed);
	memcpy(mb->buf + mb->count, buf, length);
	mb->count = needed;
}

static void free_mem_buffer(void *data)
{
	membuf_t mb = data;
	if (mb->buf != NULL) {
		unsigned char *buf = mb->buf;
		mb->buf = NULL;
		free(buf);
	}
}

static SEXP CloseMemOutPStream(R_outpstream_t stream)
{
	SEXP val;
	membuf_t mb = stream->data;
	/* duplicate check, for future proofing */
	if(mb->count > INT_MAX)
		error("serialization is too large to store in a raw vector");
	PROTECT(val = allocVector(RAWSXP, mb->count));
	memcpy(RAW(val), mb->buf, mb->count);
	free_mem_buffer(mb);
	UNPROTECT(1);
	return val;
}

SEXP My_unserialize(SEXP icon) {
	struct membuf_st mbs;
	struct R_inpstream_st in;
	if (TYPEOF(icon) != RAWSXP) {
		error("Expected RAWSXP as input");
	}

	mbs.count = 0;
	mbs.size = LENGTH(icon);
	mbs.buf = RAW(icon);
	R_InitInPStream(&in, (R_pstream_data_t) &mbs, R_pstream_any_format,
			InCharMem, InBytesMem, NULL, R_NilValue);
	return R_Unserialize(&in);
}

SEXP My_serialize(SEXP object, SEXP ascii) {
	struct R_outpstream_st out;
	R_pstream_format_t type;

	if (asLogical(ascii)) type = R_pstream_ascii_format;
	else type = R_pstream_xdr_format; /**** binary or ascii if no XDR? */
//	RCNTXT cntxt;
	struct membuf_st mbs;
	SEXP val;

	/* set up a context which will free the buffer if there is an error */
//	begincontext(&cntxt, CTXT_CCODE, R_NilValue, R_BaseEnv, R_BaseEnv,
//			R_NilValue, R_NilValue);
//	cntxt.cend = &free_mem_buffer;
//	cntxt.cenddata = &mbs;

	mbs.count = 0;
	mbs.size = 0;
	mbs.buf = NULL;
    R_InitOutPStream(&out, (R_pstream_data_t) &mbs, type, 0,
				             OutCharMem, OutBytesMem, NULL, R_NilValue);

	R_Serialize(object, &out);

	val =  CloseMemOutPStream(&out);

	/* end the context after anything that could raise an error but before
	 *        calling OutTerm so it doesn't get called twice */
//	endcontext(&cntxt);

	return val;

}

/* **********************************************************************
 * *********************************************************************/

#if 0
/* From "sockconn.c" 
 * Given an R connection, return the associated socket.
 */
SEXP SocketFromConn(SEXP s) {
	Rconnection con;
	Rsockconn this;

	if (s == R_NilValue) return R_NilValue;
	con  = getConnection(INTEGER(s)[0]);
	this = (Rsockconn) con->private;
	if (this == NULL) return R_NilValue;
	return (ScalarInteger(this->fd));
}
#endif

/* Sets a value of a variable and is called from R scripts */
void my_setVar(SEXP symbol, SEXP value, SEXP rho) {
	Rf_setVar(symbol, value, rho);
}

/* *************************************************************************
 * *************************************************************************
 * Debugging functions.
 * *************************************************************************
 * *************************************************************************
 */
                                                                                
int Decompose1( SEXP sxIn );
                                                                                
/* Decompose function from rfunctions.c */
int Decompose0( SEXP sxIn ) {
    Decompose1( (SEXP) -1 );
    Decompose1( sxIn );
    return 0;
}
                                                                                
#define VERBOSE_DECOMPOSE
/* #define INDENT_DECOMPOSE */
                                                                                
#ifndef INDENT_DECOMPOSE
#define INDENT(x)
#else
#define INDENT(x) { int IDN_LOOP##__LINE__; for (IDN_LOOP##__LINE__ = 0; IDN_LOOP##__LINE__ < (x); IDN_LOOP##__LINE__++) printf("   "); }
#endif
#ifndef min
#define min(a, b) (((a) < (b)) ? (a) : (b))
#endif

int Decompose1( SEXP sxIn ) {
    int i;
    static int iNumCalls;
    static int iIndent;
    int iNumThisCall;
    int iIndentThisCall;
    const char *cpTYPES[] = {
    "NILSXP", /*    = 0,    * nil = NULL */
    "SYMSXP", /*    = 1,    * symbols */
    "LISTSXP", /*   = 2,    * lists of dotted pairs */
    "CLOSXP", /*    = 3,    * closures */
    "ENVSXP", /*    = 4,    * environments */
    "PROMSXP", /*   = 5,    * promises: [un]evaluated closure arguments */
    "LANGSXP", /*   = 6,    * language constructs (special lists) */
    "SPECIALSXP", /*    = 7,    * special forms */
    "BUILTINSXP", /*    = 8,    * builtin non-special forms */
    "CHARSXP", /*   = 9,    * "scalar" string type (internal only)*/
    "LGLSXP", /*    = 10,   * logical vectors */
    "", "",   /* These strings intentionally left blank */
    "INTSXP", /*    = 13,   * integer vectors */
    "REALSXP", /*   = 14,   * real variables */
    "CPLXSXP", /*   = 15,   * complex variables */
    "STRSXP", /*    = 16,   * string vectors */
    "DOTSXP", /*    = 17,   * dot-dot-dot object */
    "ANYSXP", /*    = 18,   * make "any" args work */
    "VECSXP", /*    = 19,   * generic vectors */
    "EXPRSXP", /*   = 20,   * expressions vectors */
    "BCODESXP", /*    = 21,   * byte code */
    "EXTPTRSXP", /*   = 22,   * external pointer */
    "WEAKREFSXP" /*  = 23,   * weak reference */
};
                                                                                
                                                                                
    if (-1 == (int) sxIn) {
        iNumCalls = 0;
        iIndent = 0;
        return 0;
    }
                                                                                
    if ( R_NilValue == sxIn ) return 0;
                                                                                
    iNumThisCall = ++iNumCalls;
    iIndentThisCall = ++iIndent;
                                                                                
#ifdef VERBOSE_DECOMPOSE
    printf("Decomposing object:  Call %d, object type %d (%s)\n", iNumCalls, TYPEOF( sxIn ),
            cpTYPES[TYPEOF( sxIn)] );
#endif
                                                                                
    if (ATTRIB( sxIn ) != R_NilValue) {
#ifdef VERBOSE_DECOMPOSE
        printf("Attr: ");
#endif
        Decompose1( ATTRIB(sxIn) );
    }
                                                                                
    switch ( TYPEOF( sxIn ) ) {
    case 0:
        /* NILSXP */
        break;
    case 1:
        /* SYMSXP */
        Decompose1( PRINTNAME(sxIn) );
        /*  Decompose1( SYMVALUE(sxIn) ) causes an infinite loop */
        break;
    case 2: /* LISTSXP */
    /*  printf("List.  Len = %d\n", LENGTH(sxIn) ); */
    case 3: /* CLOSXP */
    case 6: /* LANGSXP */
        /* printf("Dotted Pair.  Len = %d.   ???ing...\n", LENGTH( sxIn) ); */
        if (TAG( sxIn ) != R_NilValue) {
#ifdef VERBOSE_DECOMPOSE
            printf("Tag: ");
#endif
            Decompose1( TAG( sxIn ) );
        }
        printf("Car: ");
        Decompose1( CAR( sxIn ) );
        printf("Cdr: ");
        Decompose1( CDR( sxIn ) );
        break;
/*  case 3:
 *      printf("Closure.  ???ing...\n");
 *      if (TAG( sxIn ) != R_NilValue)
 *          Decompose1( TAG( sxIn ) );
 *
 *      break;
 *  case 6:
 *      printf("Language list...\n");
 *      if (TAG( sxIn ) != R_NilValue)
 *          Decompose1( TAG( sxIn ) );
 *      Decompose1( CAR( sxIn ) );
 *      Decompose1( CDR( sxIn ) );
 *
 *      break;
 */
    case 9:
        /* CHARSXP */
        INDENT( iIndent );
        printf("\"%s\"\n", CHAR(sxIn));
        break;
    case 10:
        /* LGLSXP */
        INDENT( iIndent );
        printf("Logical array of length %d:\n", LENGTH(sxIn) );
        printf("!!! TODO   Now what? \n");
        break;
    case 13:
        /* INTSXP */
        INDENT( iIndent );
        printf("Integer array of length %d:\n", LENGTH(sxIn) );
        INDENT( iIndent );
        for (i = 0; i < min(LENGTH(sxIn), 10); i++)
            printf("%d  ", INTEGER(sxIn)[i]);
        printf("\n");
        break;
    case 14:
        /* REALSXP */
        INDENT( iIndent );
        printf("Real array of length %d:\n", LENGTH(sxIn) );
        INDENT( iIndent );
        for (i = 0; i < min(LENGTH(sxIn), 10); i++)
            printf("%f  ", REAL(sxIn)[i]);
        printf("\n");
        break;
    /* case 15: */
        /* CPLXSXP */
        break;
    case 16:
        /* STRSXP */
        INDENT( iIndent );
        printf("String array of length %d:\n", LENGTH(sxIn) );
        for (i = 0; i < LENGTH(sxIn); i++)
            Decompose1( STRING_PTR(sxIn)[i] );
        printf("\n");
        break;
    case 19:
        /* VECSXP */
#ifdef VERBOSE_DECOMPOSE
        printf("Generic vector of length %d:\n", LENGTH(sxIn) );
#endif
        for (i = 0; i < LENGTH(sxIn); i++)
            Decompose1( VECTOR_PTR(sxIn)[i] );
        break;
    default:
        printf("Case of '%d' not yet handled.\n", TYPEOF( sxIn ) );
    }
#ifdef VERBOSE_DECOMPOSE
    printf("Finished decomposing object:  Call %d, object type %d (%s)\n", iNumThisCall, TYPEOF( sxIn ),
            cpTYPES[TYPEOF( sxIn)] );
#endif
                                                                                
    --iIndent;
    return 0;
}

