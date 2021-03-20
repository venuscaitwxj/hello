#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include "timing.h"
#define N 1500 /* matrix size */

int main (int argc, char *argv[]) 
{
int	i, j, k;
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
  int B = N / 15;
  int ii,jj,kk;
  for (ii = 0; ii < N; ii+=B) {
    for (jj = 0; jj < N; jj+=B) {
      for (kk = 0; kk < N; kk+=B) {
        for (i = ii; i < ii+B; i++) {
          for (j = jj; j < jj+B; j++) {
            for (k = kk; k < kk+B; k+=2) {
              c[i][j] += a[i][k]*b[k][j];
              c[i][j] += a[i][k+1]*b[k+1][j];
            }
          }
        }
      }
    }
  }



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



