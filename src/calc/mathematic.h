// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef _CALC_MATHEMATIC_H_
#define _CALC_MATHEMATIC_H_

namespace dingodb::expr::calc
{

double Ceil(double v);

double Floor(double v);

double Sin(double v);

double Cos(double v);

double Tan(double v);

double Asin(double v);

double Acos(double v);

double Atan(double v);

double Sinh(double v);

double Cosh(double v);

double Tanh(double v);

double Exp(double v);

double Log(double v);

} // namespace dingodb::expr::calc

#endif /* _CALC_MATHEMATIC_H_ */
