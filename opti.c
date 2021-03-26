#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include "timing.h"
#include <string.h>
#include <omp.h>
#define N 1500 /* matrix size */

int main (int argc, char *argv[]) 
{
int	i, j, k;
//int N = 1500;
timing_t tstart, tend;
double	a[N][N],           /* matrix A to be multiplied */
	b[N][N],           /* matrix B to be multiplied */
	c[N][N];           /* result matrix C */



  for (i=0; i<N; i++)
    for (j=0; j<N; j++)
      a[i][j]= i+j;

  for (i=0; i<N; i++)
    for (j=0; j<N; j++)
      b[i][j]= i*j;

  for (i=0; i<N; i++)
    for (j=0; j<N; j++)
      c[i][j]= 0;


  get_time(&tstart);
/*
  for (i=0; i<N; i++)
    {
    for(j=0; j<N; j++){
      for (k=0; k<N; k+=2) {
        c[i][j] += a[i][k] * b[k][j];
        c[i][j] += a[i][k+1] * b[k+1][j];

        }
      }
    }
*/

 //Blocking + unrolling innermost 2x
  int blockSize = N / 3;
  int iInner, kInner,jInner;

  #pragma omp parallel shared(blockSize,a,b,c,N) private(i,j,k,iInner,kInner,jInner)
  #pragma omp for
  for (i = 0; i < N; i+=blockSize)
    for (k=0; k<N ; k+= blockSize)
      for (j = 0 ; j < N; j+=blockSize)   
        for (iInner = i; iInner<j+blockSize; iInner+=2)     
          for (kInner = k ; kInner<k+blockSize; kInner++)
            for (jInner = j ; jInner<j+blockSize ; jInner++)
              c[iInner][jInner] += a[iInner][kInner] *b[kInner][jInner];
              c[iInner+1][jInner] += a[iInner+1][kInner] *b[kInner][jInner];


  
    get_time(&tend);



  printf("*****************************************************\n");

  double c_F = 0.0;
  for (i=0; i<N; i++) {
      for (j=0; j<N; j++) {
          c_F += c[i][j] * c[i][j];
          //printf("%6.2f   ", c[i][j]);
      }
      //printf("\n");
  }
  printf("||c||_F = %6.2f\n", sqrt(c_F));
  printf ("Elapsed Time: %g s.\n", timespec_diff(tstart,tend));
}



