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

  #pragma acc data copy(c[0:N][0:N]) copyin(a[0:N][0:N]) copyin(b[0:N][0:N])
  #pragma acc kernels loop independent
  //#pragma acc parallel loop //another version

  for (i=0; i<N; i++)
    {
    for(j=0; j<N; j++){
      for (k=0; k<N; k++) {
        c[i][j] += a[i][k] * b[k][j];

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



